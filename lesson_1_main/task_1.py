import pandas as pd


def main():
    df = pd.read_csv("log.tsv", sep="\t", usecols=["userid", "timestamp"])
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.date

    users = df.groupby("timestamp")["userid"].nunique().reset_index()
    users = users.sort_values("timestamp", ascending=True)

    for row in users.itertuples(index=False):
        print(f"{row.timestamp} {row.userid}")


if __name__ == '__main__':
    main()
