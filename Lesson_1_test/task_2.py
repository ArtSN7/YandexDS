import csv
import re


def main():
    input_name = "ticket_logs.csv"

    data = {}

    with open(input_name, "r") as f:

        reader = csv.reader(f)

        for row in reader:
            if row[0] not in data:
                s = clear_str(row[1])
                if len(s) == 11:
                    data[row[0]] = [s]
            else:

                val = clear_str(row[1])
                if val not in data[row[0]] and len(val) == 11:
                    data[row[0]].append(val)

        longest_list_key = max(data, key=lambda k: len(data[k]))
        longest_list = data[longest_list_key]

        print(len(longest_list))


def clear_str(s):
    return re.sub(r'\D', '', s)


if __name__ == "__main__":
    main()
