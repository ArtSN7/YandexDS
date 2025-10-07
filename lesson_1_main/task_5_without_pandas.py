from datetime import datetime
import csv


def is_less_than_30_minutes(timestamp1, timestamp2):
    date1 = datetime.fromisoformat(timestamp1.replace('Z', '+00:00'))
    date2 = datetime.fromisoformat(timestamp2.replace('Z', '+00:00'))

    time_diff = abs(date2 - date1)

    return time_diff.total_seconds() <= 1800


def main():
    response = []

    sessions_data = {}  # связывает user_id and session_id
    current_session_id = 0
    timestamps_data = {}  # связывает user_id с его последним заходом

    with open('log.tsv', 'r', newline='') as file:
        reader = csv.DictReader(file, delimiter='\t')

        # пример row: {'userid': 'user_1', 'timestamp': '2022-08-22T10:00:00', 'action': 'product', 'value': '0', 'testids': '13534;23345;23463;25662'}
        for row in reader:  # проходимся по каждому логу из даты

            userid = row['userid']

            if userid not in sessions_data:  # если у пользователя еще не было sessions

                current_session_id += 1  # делаем новый уникальный session_id для пользователя
                sessions_data[userid] = current_session_id  # связываем сессию и юзера
                timestamps_data[userid] = row['timestamp']  # обновляем последний заход юзера

            else:  # если у юзера уже есть сессия ( надо проверить наскок давно было )

                last_timestamp = timestamps_data[userid]  # последний заход когда был
                newest_timestamp = row['timestamp']  # заход сейчас

                if is_less_than_30_minutes(last_timestamp, newest_timestamp):  # если у юзера сессия продолжается
                    timestamps_data[userid] = row['timestamp']  # обновляем последний заход юзера

                else:
                    timestamps_data[userid] = row['timestamp']
                    current_session_id += 1  # делаем новый session_id
                    sessions_data[userid] = current_session_id  # обновляем session_id

            sesid = sessions_data[userid]  # обновляем session_id для юзера
            add_dict = {'sessionid': sesid}
            response.append(row | add_dict)

    with open("output.tsv", 'w', newline='', encoding='utf-8') as tsvfile:
        writer = csv.DictWriter(tsvfile, fieldnames=["userid", "timestamp", "action", "value", "testids", "sessionid"],
                                delimiter='\t')
        writer.writeheader()  # Write the header row
        writer.writerows(response)  # Write the data rows


if __name__ == '__main__':
    main()
