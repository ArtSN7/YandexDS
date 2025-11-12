def main():
    TP = int(input())
    FP = int(input())
    TN = int(input())
    FN = int(input())

    Precision = TP / (TP + FP)
    Recall = TP / (TP + FN)
    Accuracy = (TP + TN) / (TP + FP + TN + FN)

    def fmt(x):
        return f"{x:.4f}".rstrip('0').rstrip('.')

    print(f"{fmt(Precision)}\n{fmt(Recall)}\n{fmt(Accuracy)}")


if __name__ == '__main__':
    main()
