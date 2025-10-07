import pandas as pd


SESSION_TIMEOUT = 1800


def assign_sessions(df):
    df = df.sort_values(by=["userid", "timestamp"]).copy()

    # Инициализация столбца sessionid
    df['sessionid'] = 0

    # Векторизованное вычисление sessionid
    session_id = 1
    for userid, group in df.groupby('userid'):
        # Вычисляем разницу во времени между последовательными записями
        time_diffs = group['timestamp'].diff().dt.total_seconds().abs()

        # Первая запись в группе всегда начинает новую сессию
        session_ids = [session_id]

        # Определяем сессии на основе разницы во времени
        for diff in time_diffs[1:]:
            if diff > SESSION_TIMEOUT:
                session_id += 1
            session_ids.append(session_id)

        # Присваиваем sessionid для группы
        df.loc[group.index, 'sessionid'] = session_ids

        # Обновляем session_id для следующей группы
        session_id += 1

    return df.sort_index()


def main():
    df = pd.read_csv('log.tsv', sep='\t', parse_dates=['timestamp'], date_format='ISO8601')
    # Присваивание сессий
    df = assign_sessions(df)
    df.to_csv('output.tsv', sep='\t', index=False)


if __name__ == '__main__':
    main()
