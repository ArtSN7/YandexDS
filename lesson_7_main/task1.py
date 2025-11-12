import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def load_data(filepath: str) -> pd.DataFrame:
    data = pd.read_csv(filepath)
    data["date"] = pd.to_datetime(data["date"])
    return data


def get_percentiles(data):
    percentiles = [5, 10, 25, 50, 75, 90, 95]
    response = {}
    for p in percentiles:
        value = np.percentile(data["duration"], p)
        response[p] = value

    return response


def analyse_outliers(data: pd.DataFrame):
    q1 = data["duration"].quantile(0.25)
    q3 = data["duration"].quantile(0.75)
    iqr = q3 - q1

    outlier_threshold = q3 + 1.5 * iqr # no q1 cos ain't < 0 
    outliers = data[data["duration"] > outlier_threshold]

    return outliers


def create_duration_buckets(data, bins, labels):
    data["duration_bucket"] = pd.cut(
        data["duration"], bins=bins, labels=labels, include_lowest=True
    )

    duration_distribution = (
        data.groupby("duration_bucket", observed=True)
        .size()
        .reset_index(name="sessions_count")
    )
    duration_distribution["percentage"] = (
        duration_distribution["sessions_count"] / len(data) * 100
    )

    return duration_distribution


def convert_seconds_to_minutes(seconds):
    minutes = seconds // 60

    return f"{int(minutes)}"


def main():
    data = load_data("./sessions.csv")

    percentiles_data = get_percentiles(data)
    # outliers = analyse_outliers(data)

    DURATION_BINS = [
        0,
        percentiles_data[5],
        percentiles_data[10],
        percentiles_data[25],
        percentiles_data[50],
        percentiles_data[75],
        percentiles_data[90],
        percentiles_data[95],
        float("inf"),
    ]

    DURATION_LABELS = [
        f"< {convert_seconds_to_minutes(percentiles_data[5])} мин)",
        f"{convert_seconds_to_minutes(percentiles_data[5])}-{convert_seconds_to_minutes(percentiles_data[10])} мин",
        f"{convert_seconds_to_minutes(percentiles_data[10])}-{convert_seconds_to_minutes(percentiles_data[25])} мин",
        f"{convert_seconds_to_minutes(percentiles_data[25])}-{convert_seconds_to_minutes(percentiles_data[50])} мин",
        f"{convert_seconds_to_minutes(percentiles_data[50])}-{convert_seconds_to_minutes(percentiles_data[75])} мин",
        f"{convert_seconds_to_minutes(percentiles_data[75])}-{convert_seconds_to_minutes(percentiles_data[90])} мин",
        f"{convert_seconds_to_minutes(percentiles_data[90])}-{convert_seconds_to_minutes(percentiles_data[95])} мин",
        f"> {convert_seconds_to_minutes(percentiles_data[95])} мин",
    ]

    duration_distribution = create_duration_buckets(data, DURATION_BINS, DURATION_LABELS)

    duration_distribution.plot.barh(
        x="duration_bucket", y="sessions_count", figsize=(15, 5), label='Количество сессий', color='skyblue'
    )

    plt.show()


if __name__ == "__main__":
    main()
