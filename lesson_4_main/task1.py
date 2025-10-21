import pandas as pd


def process(df):
    session_times = df.groupby(['user_id', 'session_id'])['timestamp'].agg(['min', 'max'])
    session_times['duration'] = session_times['max'] - session_times['min']

    cleared_sessions = session_times[session_times['duration'] >= 1]

    start_events = df.merge(cleared_sessions[['min']], left_on=['user_id', 'session_id', 'timestamp'],
                            right_on=['user_id', 'session_id', 'min'])

    mainpage_starts = start_events[start_events['action'] == 'mainpage'][['user_id', 'session_id']].drop_duplicates()

    return round((mainpage_starts.shape[0] / cleared_sessions.shape[0]) * 100, 2)


if __name__ == '__main__':
    df = pd.read_csv('./data.tsv', sep='\t')
    print(process(df))
