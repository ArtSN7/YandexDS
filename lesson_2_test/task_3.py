import dataclasses
from simplemr import SimpleMapReduce
from datetime import datetime


@dataclasses.dataclass
class TimeValue:
    value: float
    date: datetime


@dataclasses.dataclass
class Response:
    month: datetime
    value: float


def clearing_data(line):
    data = line.split("\t")

    if data[2] != "checkout":
        yield from ()
    else:
        yield TimeValue(value=float(data[3]), date=datetime.fromisoformat(data[1].replace("Z", "+00:00")).strftime("%Y-%m"))


def calc_groups(item):
    month = ''
    value = 0.0

    for i in item:
        month = i.date
        value += float(i.value)

    yield Response(month=month, value=value)


def process(mrjob: SimpleMapReduce) -> SimpleMapReduce:
    return (
        mrjob
        .map(clearing_data)  # make dates to be for year-month | have only checkout events | add value from the table
        .reduce(calc_groups, key=["date"])
    )
