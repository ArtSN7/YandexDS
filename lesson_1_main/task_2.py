import pandas as pd


def parse_data(data):
    a = {}
    for i in list(map(lambda x: x.strip("\n"), data.split("\t"))):
        a[i.split("=")[0]] = i.split("=")[1]

    return a


def main():
    rows = []
    with open('log.dsv', 'r') as file:
        lines = file.readlines()

        for line in lines:
            resp = parse_data(line)

            rows.append([resp["userid"], resp["timestamp"], resp["action"], resp["value"], resp["testids"]])

    df = pd.DataFrame(rows, columns=['userid', 'timestamp', 'action', 'value', 'testids']).drop_duplicates()
    df.to_csv('output.tsv', sep='\t', index=False, header=True, float_format='%.1f')


if __name__ == '__main__':
    main()
