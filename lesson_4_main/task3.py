import pandas as pd
import numpy as np


# function that calcs BMI by passed H and W
def calc_bmi(h, w):
    h_meters = format_h(h)
    w_kg = format_w(w)

    return w_kg / (h_meters ** 2)


def format_h(h):
    feet, inches = h.split("'")
    feet = int(feet)
    inches = int(inches.replace('"', '').strip())  # Remove " and whitespace if present
    return (feet * 0.3048) + (inches * 0.0254)


def format_w(w):
    return int(w.replace('lbs', '').strip()) * 0.45359237


# Cal Normality for each player
def calculate_normality(row, means):
    deviations_squared = [
        (row['Age'] - means['Age']) ** 2,
        (row['Weight'] - means['Weight']) ** 2,
        (row['Height'] - means['Height']) ** 2,
        (row['Overall'] - means['Overall']) ** 2,
        (row['BMI'] - means['BMI']) ** 2
    ]
    return np.sqrt(sum(deviations_squared))


def process(df):
    # clean data and assign BMI
    df_cleaned = df.dropna(subset=['Name', 'Age', 'Height', 'Weight', 'Overall']).drop_duplicates()
    df_cleaned = df_cleaned[['ID', 'Name', 'Age', 'Height', 'Weight', 'Overall']]
    df_cleaned['BMI'] = df_cleaned.apply(lambda row: calc_bmi(row['Height'], row['Weight']), axis=1)

    # create new df with converted metric to normal standarts
    df_converted = df_cleaned.copy()
    df_converted['Height'] = df_converted.apply(lambda row: format_h(row['Height']), axis=1)
    df_converted['Weight'] = df_converted.apply(lambda row: format_w(row['Weight']), axis=1)

    # calculate means and then normality
    means = df_converted[['Age', 'Weight', 'Height', 'Overall', 'BMI']].mean()
    df_converted['Normality'] = df_converted.apply(lambda row: calculate_normality(row, means), axis=1)

    # sort out by normality and slice first 3 values
    df_converted = df_converted.sort_values(by=['Normality'], ascending=True).head(3).reset_index(drop=True)
    df_converted = df_converted[['Weight', 'Height', 'Age', 'Overall', 'BMI', 'Name', 'Normality']]

    return df_converted


if __name__ == '__main__':
    df = pd.read_csv('./FIFA 21 Official Data.csv', sep=',')
    print(process(df))
