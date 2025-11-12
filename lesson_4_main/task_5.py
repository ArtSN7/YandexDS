import pandas as pd
from collections import defaultdict


min_support = float(input())
with open("store_data.csv") as f:
    lines = f.readlines()

baskets = []
for line in lines:
    items = [item.strip() for item in line.strip().split(",") if item.strip()]
    if items:
        baskets.append(items)

total_baskets = len(baskets)
pair_counts = defaultdict(int)

for basket in baskets:
    for i in range(len(basket)):
        for j in range(len(basket)):
            if i != j:
                pair = (basket[i], basket[j])
                pair_counts[pair] += 1

results = []
for (item_a, item_b), count in pair_counts.items():
    support = count / total_baskets
    if support >= min_support:
        results.append(
            {
                "first_item": item_a,
                "second_item": item_b,
                "support": round(support, 5),
            }
        )

df_results = pd.DataFrame(results)
df_results.to_csv("output.csv", index=False)