import pandas as pd


def is_lucky_ticket(ticket):
    if pd.isna(ticket) or not isinstance(ticket, str):
        return False

    digits = ''.join(c for c in str(ticket) if c.isdigit())

    if len(digits) == 0 or len(digits) % 2 != 0:
        return False

    mid = len(digits) // 2
    first_half = digits[:mid]
    second_half = digits[mid:]

    sum_first = sum(int(d) for d in first_half)
    sum_second = sum(int(d) for d in second_half)

    return sum_first == sum_second


def process(df):
    df['is_lucky'] = df['Ticket'].apply(is_lucky_ticket)

    lucky_passengers = df[df['is_lucky']]

    survival_rate = lucky_passengers['Survived'].mean() * 100

    return round(survival_rate, 2)


if __name__ == "__main__":
    df = pd.read_csv("./Titanic Data Analysis.csv", sep="\t")
    print(process(df))
