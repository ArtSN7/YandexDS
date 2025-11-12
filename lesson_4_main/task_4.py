import pandas as pd

def process(df):
    df['R_rank'] = df['Recency'].rank(ascending=False)
    df['R'] = df['R_rank'] / df['R_rank'].max()

    df['F_rank'] = df['Frequency'].rank(ascending=True)
    df['F'] = df['F_rank'] / df['F_rank'].max()

    df['M_rank'] = df['Monetary'].rank(ascending=True)
    df['M'] = df['M_rank'] / df['M_rank'].max()

    df['RFM_Score'] = 5 * (15 * df['R'] + 28 * df['F'] + 57 * df['M']) / 100

    valuable_users = df[(df['RFM_Score'] >= 4) & (df['RFM_Score'] <= 4.5)]

    return valuable_users['Customer_ID'].tolist()