import re
import pandas as pd


def main():
    with open('user_activity_bad_log.tsv', 'r') as file:
        lines = list(map(lambda x: x.strip('\n').split('\t'), file.readlines()))

        # То когда нормальная дата по группе из трех (len == 3)
        normal_lines = list(filter(lambda x: len(x) == 3, lines))
        df_normal = pd.DataFrame(normal_lines, columns=["userid", "timestamp", "action"])

        # кейс когда userid=31313
        df_normal["userid"] = df_normal["userid"].str.replace("userid=", "", regex=False)

        # Когда что то не то с датой (len != 3 and len != 1)
        corrupted_lines = list(filter(lambda x: len(x) != 3 and len(x) != 1, lines))
        cleaned_corrupted_lines = []

        # проходимся по линиям когда они userid -> timestamp -> action + userid -> timestamp ->...
        a = ["", "", ""]
        for line in corrupted_lines[0]:
            if a[0] == "" and a[1] == "" and a[2] == "":
                a[0] = line
            elif a[1] == "" and a[2] == "":
                a[1] = line
            else:
                parts = re.match(r"([a-zA-Z]+)(\d+)", line)
                if parts:
                    text, number = parts.groups()[0], parts.groups()[1]
                    a[2] = text
                    cleaned_corrupted_lines.append(a)
                    a = ["", "", ""]
                    a[0] = number

        # Create DataFrame for cleaned corrupted data
        df_corrupted = pd.DataFrame(cleaned_corrupted_lines, columns=["userid", "timestamp", "action"])
        df_corrupted["userid"] = df_corrupted["userid"].str.replace("userid=", "", regex=False)

        # Combine normal and corrupted data
        df = pd.concat([df_normal, df_corrupted], ignore_index=True)

        # Filter for checkout or checkᝃut actions
        df = df[(df["action"] == "checkout") | (df["action"] == "checkᝃut")].drop_duplicates()

        # Calculate metrics
        unique_users = df["userid"].nunique()
        all_trans = df.shape[0]

        print(all_trans / unique_users)


if __name__ == '__main__':
    main()
