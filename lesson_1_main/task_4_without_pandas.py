import csv
from datetime import datetime
from collections import defaultdict


def read_tsv(file_path):
    data = []
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            data.append({
                'userid': row['userid'],
                'timestamp': datetime.fromisoformat(row['timestamp']).replace(hour=0, minute=0, second=0,
                                                                              microsecond=0),
                'action': row['action'],
                'value': float(row['value'])
            })
    return data


def get_reg_dates(data):
    reg_dates = {}
    for row in data:
        userid = row['userid']
        timestamp = row['timestamp']
        if userid not in reg_dates or timestamp < reg_dates[userid]:
            reg_dates[userid] = timestamp
    return reg_dates


def get_month_range(reg_dates):
    min_date = min(reg_dates.values())
    max_date = max(reg_dates.values())

    min_month = min_date.replace(day=1)
    max_month = max_date.replace(day=1)

    months = []
    current = min_month
    while current <= max_month:
        months.append(current.strftime('%Y-%m'))
        year = current.year + (current.month // 12)
        month = current.month % 12 + 1
        current = current.replace(year=year, month=month)

    return months


def process_checkouts(data, reg_dates):
    checkouts = []
    for row in data:
        if row['action'] == 'checkout':
            reg_date = reg_dates[row['userid']]
            dsr = (row['timestamp'] - reg_date).days
            week = (dsr // 7) + 1
            if 1 <= week <= 4:
                checkouts.append({
                    'cohort': reg_date.strftime('%Y-%m'),
                    'week': week,
                    'value': row['value']
                })
    return checkouts


def aggregate_data(checkouts):
    agg = defaultdict(float)
    for row in checkouts:
        key = (row['cohort'], row['week'])
        agg[key] += row['value']

    result = []
    for (cohort, week), value in agg.items():
        result.append({
            'cohort': cohort,
            'week': week,
            'gmv_mln': round(value / 1_000_000)
        })
    return result


def create_pivot(agg_data, months):
    pivot = {month: {1: 0, 2: 0, 3: 0, 4: 0} for month in months}
    for row in agg_data:
        pivot[row['cohort']][row['week']] = row['gmv_mln']
    return pivot


def print_pivot(pivot):
    for cohort in pivot:
        row = pivot[cohort]
        print(f"{cohort}\t|\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}")


def main():
    # Read and process data
    data = read_tsv('log.tsv')
    reg_dates = get_reg_dates(data)
    months = get_month_range(reg_dates)

    # Process checkouts
    checkouts = process_checkouts(data, reg_dates)

    # Aggregate data
    agg_data = aggregate_data(checkouts)

    # Create and print pivot table
    pivot = create_pivot(agg_data, months)
    print_pivot(pivot)


if __name__ == "__main__":
    main()
