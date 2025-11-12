import pandas as pd
from itertools import combinations


def main():
    min_support = float(input().strip())
    min_confidence = float(input().strip())

    with open('store_data.csv', 'r') as file:
        lines = []
        for line in file.readlines():
            lines.append(line.strip().split(','))

    baskets = [list(set(basket)) for basket in lines if basket]
    total_baskets = len(baskets)

    item_count = {}
    pair_count = {}

    for basket in baskets:

        for item in basket:
            item_count[item] = item_count.get(item, 0) + 1

        for item1, item2 in combinations(basket, 2):
            pair_count[(item1, item2)] = pair_count.get((item1, item2), 0) + 1
            pair_count[(item2, item1)] = pair_count.get((item2, item1), 0) + 1

    results = []

    for (item1, item2), count_ab in pair_count.items():
        support = count_ab / total_baskets
        count_a = item_count[item1]
        confidence = count_ab / count_a
        if support >= min_support and confidence >= min_confidence:
            results.append({
                'first_item': item1,
                'second_item': item2,
                'confidence': round(confidence, 3)
            })

    pd.DataFrame(results).to_csv('output.csv', index=False)


if __name__ == '__main__':
    main()
