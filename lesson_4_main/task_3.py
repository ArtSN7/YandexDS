import pandas as pd


def process(df):
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
    max_date = df['Date'].max()

    rfm = df.groupby('Customer_ID').agg({
        'Date': lambda x: (max_date - x.max()).days,
        'Transaction_ID': 'count',
        'Sales_Amount': 'sum'
    }).reset_index()

    rfm.columns = ['Customer_ID', 'Recency', 'Frequency', 'Monetary']

    rfm['Monetary'] = rfm['Monetary'].round(2)

    rfm = rfm.sort_values('Customer_ID').reset_index(drop=True)

    return rfm


if __name__ == '__main__':
    df = pd.read_csv('./customer_transactions_log.csv', sep=',')
    result = process(df)
    print(result)