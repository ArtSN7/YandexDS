import pandas as pd


def process(df1, df2):
    df1_indexed = df1.set_index('ID')
    df2_indexed = df2.set_index('ID')
    result = df1_indexed.combine_first(df2_indexed).reset_index()
    result.dropna(axis=0, how='any', inplace=True)
    cols = [col for col in result.columns if col != 'ID'] + ['ID']
    result = result[cols]
    return result


if __name__ == '__main__':
    df1 = pd.read_csv('./FIFA22_broken.csv', sep=',')
    df2 = pd.read_csv('./FIFA23_broken.csv', sep=',')
    print(process(df1, df2))
