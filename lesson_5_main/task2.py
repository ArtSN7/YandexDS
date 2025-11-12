"""
Precision = TP / (TP + FP)
Recall = TP / (TP + FN)
"""

import pandas as pd
import numpy as np


def main():
    df = pd.read_csv(input())

    thresholds = np.arange(0.1, 1.0, 0.1)

    for thresh in thresholds:
        df["preds"] = (df["prediction"] >= thresh).astype(int)
        TP = ((df["preds"] == 1) & (df["target"] == 1)).sum()
        FP = ((df["preds"] == 1) & (df["target"] == 0)).sum()
        FN = ((df["preds"] == 0) & (df["target"] == 1)).sum()

        precision = TP / (TP + FP) if (TP + FP) else 0.0

        recall = TP / (TP + FN) if (TP + FN) else 0.0

        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall)
            else 0.0
        )

        def fmt(x):
            return f"{x:.4f}".rstrip("0").rstrip(".")

        print(f"{fmt(precision)} {fmt(recall)} {fmt(f1)}")


if __name__ == "__main__":
    main()
