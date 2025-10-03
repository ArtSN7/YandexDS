import json
import csv


def read_json_file(file_name):
    try:
        with open(file_name, 'r') as json_file:
            data = json.load(json_file)

            data = sorted(data, key=lambda k: k['date'], reverse=True)
            return data

    except FileExistsError as e:
        return e


def writing_to_csv(file_name, data):
    with open(file_name, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["creature", "place", "danger", "date"])
        for i in data:
            writer.writerow([i["creature"], i["place"], i["danger"], i["date"]])


def main():
    input_file_name = "cases.json"
    output_file_name = "incidents.csv"

    data = read_json_file(input_file_name)

    writing_to_csv(output_file_name, data)


if __name__ == "__main__":
    main()
