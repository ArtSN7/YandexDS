import dataclasses
from simplemr import SimpleMapReduce


@dataclasses.dataclass
class User:
    userid: str
    date: str


@dataclasses.dataclass
class PurchCount:
    userid: str
    frequency: int


def allocate_groups(item):
    userid = ""
    fr = 0
    for i in item:
        userid = i.userid
        fr += 1

    yield PurchCount(userid, fr)


def clear_data_by_checkout(line):
    data = line.split("\t")
    if data[2] != "checkout":
        yield from ()
    else:
        yield User(userid=data[0], date=data[1])


def process(mrjob: SimpleMapReduce) -> SimpleMapReduce:
    return (
        mrjob
        .map(clear_data_by_checkout)  # сортируем юзеров у кого есть checkout
        .reduce(allocate_groups, key=["userid"])
    )
