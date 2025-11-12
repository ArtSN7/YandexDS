import pandas as pd


def parse_time_to_minutes(time_val):
    """Преобразует время в минуты от начала суток"""
    # Проверяем на NaN
    if pd.isna(time_val):
        return pd.NA

    # Преобразуем в строку
    time_str = str(time_val).strip()

    if time_str in ['', 'nan', 'NaN', 'None']:
        return pd.NA

    try:
        # Удаляем префиксы типа "c:", "c :", "C:", "c: " и т.д.
        # Ищем паттерн: буквы + двоеточие + возможный пробел в начале
        clean_time = time_str

        # Если начинается с буквы
        if clean_time and clean_time[0].isalpha():
            # Находим первую цифру
            first_digit_pos = -1
            for i, char in enumerate(clean_time):
                if char.isdigit():
                    first_digit_pos = i
                    break

            # Если нашли цифру, берем все начиная с нее
            if first_digit_pos > 0:
                clean_time = clean_time[first_digit_pos:]

        # Убираем лишние пробелы
        clean_time = clean_time.strip()

        if not clean_time:
            return pd.NA

        # Теперь парсим время
        hours = None
        minutes = 0

        # Формат HH:MM или H:MM
        if ':' in clean_time:
            parts = clean_time.split(':')
            hours = int(parts[0].strip())
            if len(parts) > 1 and parts[1].strip():
                minutes = int(parts[1].strip()[:2])
        # Формат HHMM (4 цифры)
        elif len(clean_time) == 4 and clean_time.isdigit():
            hours = int(clean_time[:2])
            minutes = int(clean_time[2:4])
        # Формат HMM (3 цифры)
        elif len(clean_time) == 3 and clean_time.isdigit():
            hours = int(clean_time[0])
            minutes = int(clean_time[1:3])
        # Формат HH или H (только часы)
        elif clean_time.isdigit():
            hours = int(clean_time)
            minutes = 0

        # Проверяем, что удалось извлечь часы
        if hours is None:
            return pd.NA

        # Проверяем корректность
        if 0 <= hours < 24 and 0 <= minutes < 60:
            return hours * 60 + minutes
        return pd.NA

    except (ValueError, IndexError, AttributeError):
        return pd.NA


def process(df):

    df =df.dropna(subset=['Time', 'Fatalities']).reset_index(drop=True)
    df = df[df['Fatalities'] > 0].reset_index(drop=True)
    print(df['Time'])

    df['time_minutes'] = df['Time'].apply(parse_time_to_minutes)

    df = df[df['time_minutes'].notna()].copy()

    morning_start = 8 * 60
    morning_end = 10 * 60

    evening_start = 20 * 60
    evening_end = 22 * 60

    df['Fatalities_num'] = pd.to_numeric(df['Fatalities'], errors='coerce').fillna(0)

    morning_fatalities = df.loc[
        (df['time_minutes'] >= morning_start) & (df['time_minutes'] <= morning_end), 'Fatalities_num'].sum()
    evening_fatalities = df.loc[
        (df['time_minutes'] >= evening_start) & (df['time_minutes'] <= evening_end), 'Fatalities_num'].sum()

    ratio = morning_fatalities / evening_fatalities

    return round(ratio, 4)


if __name__ == "__main__":
    df = pd.read_csv("./Airplane Crushes Data Analysis.csv", sep=",")
    result = process(df)
    print(result)
