import os
import json
from yt.wrapper import YtClient


def main():
    YT_PROXY = os.getenv("YT_PROXY")
    with open("config.json", "r") as f:
        config = json.load(f)
    client = YtClient(proxy=YT_PROXY, config=config)

    response = []  # [[table1, chunks, size], [...]] - array for printing out

    # @chunk_count - to count num of chunks for response
    # @compressed_data_size - to get table size - check for compression>>>

    path = '//home/data/tutorial/ytsaurus_intro/examine/another_table'  # total path for the nodes values
    all_nodes = client.list(path=path)

    for i in all_nodes:
        inner_list = [i, '', '']
        new_path = f'{path}/{i}'
        inner_list[1] = client.get(f'{new_path}/@chunk_count')
        inner_list[2] = client.get(f'{new_path}/@compressed_data_size')
        response.append(inner_list)

    for i in response:
        print(f'{i[0]} {i[1]} {i[2]}')


if __name__ == "__main__":
    main()
