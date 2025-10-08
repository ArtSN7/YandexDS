import dataclasses
from datetime import datetime

from simplemr import SimpleMapReduce


@dataclasses.dataclass
class UserDatePair:
    userid: str
    date: datetime


@dataclasses.dataclass
class DAU:
    date: datetime
    dau: int


# кладем в рекорд userid и обрезанную дату
def assign_to_record(line):
    fields = line.strip().split("\t")

    if fields[2] != "search":  # заголовок
        yield from ()

    else:
        yield UserDatePair(userid=fields[0], date=datetime.fromisoformat(fields[1]).strftime("%Y-%m-%d"))


def utilise_data(record):
    for user_id in record:
        yield user_id
        break


def count_dau(item):
    count = 0
    data = None

    for i in item:
        data = i.date
        count += 1

    yield DAU(date=data, dau=count)


def process(mrjob: SimpleMapReduce) -> SimpleMapReduce:
    return (
        mrjob
        .map(assign_to_record)
        .reduce(utilise_data, key=["userid", "date"])
        .reduce(count_dau, key=["date"])
    )
