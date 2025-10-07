import pandas as pd


def main():
    df = pd.read_csv('log.tsv', sep="\t")
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.floor("D")  # перевод всего в datetime до дня
    reg_dates = df.groupby("userid")["timestamp"].min()

    min_month = reg_dates.min().to_period("M")
    max_month = reg_dates.max().to_period("M")
    months = pd.period_range(min_month, max_month, freq="M").astype(
        str)  # создаем группы по месяцам по min and max months

    df["reg_date"] = df["userid"].map(reg_dates)
    df["cohort"] = df["reg_date"].dt.to_period("M").astype(str)

    checkouts = df[df["action"] == "checkout"].copy()

    checkouts["dsr"] = (checkouts["timestamp"].dt.floor("D") - checkouts["reg_date"]).dt.days
    checkouts["week"] = checkouts["dsr"] // 7 + 1

    checkouts = checkouts[checkouts["week"].between(1, 4)]

    agg = checkouts.groupby(["cohort", "week"], as_index=False)["value"].sum()

    agg["gmv_mln"] = (agg["value"] / 1_000_000).round()

    pivot = agg.pivot(index="cohort", columns="week", values="gmv_mln")

    pivot = pivot.reindex(index=months)

    for week in range(1, 5):
        if week not in pivot.columns:
            pivot[week] = 0
    pivot = pivot[[1, 2, 3, 4]].fillna(0)

    for i in months:
        row = pivot.loc[i]
        print(f"{i}\t|\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}")


if __name__ == "main":
    main()
