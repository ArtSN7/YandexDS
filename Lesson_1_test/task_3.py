import csv


class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):

        if x not in self.parent:
            self.parent[x] = x

        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])

        return self.parent[x]

    def union(self, x, y):

        #  соединяем только главные значени графа ( самые верхние )
        px = self.find(x)
        py = self.find(y)

        if px != py:
            self.parent[py] = px  # меняем местами


def main():
    uf = UnionFind()

    count_by_account = {}

    with open('test_trans_log.csv', 'r') as file:
        reader = csv.reader(file, delimiter=",")

        for row in reader:

            email, phone = row

            uf.union(email, phone)

            account = uf.find(email)  # find unique users' email - like the parent email

            if account not in count_by_account:
                count_by_account[account] = 1
            else:
                count_by_account[account] += 1

    # в parents у нас теперь все связано, но из за незнания некоторых связей мы добавляли некоторые emails
    # как уникальные, поэтому нужно пройтись и собрать все данные вместе

    for i in count_by_account:
        parent_class = uf.find(i)

        if parent_class != i:
            count_by_account[parent_class] += count_by_account[i]

    print(max(count_by_account.values()))


if __name__ == "__main__":
    main()
