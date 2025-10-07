from typing import Iterable, Callable
from itertools import groupby


def flatten[T](inp: Iterable[Iterable[T]]) -> Iterable[T]:
    for it in inp:
        for item in it:
            yield item


def run_map[T, U](mapper: Callable[T, Iterable[U]], input_stream: Iterable[T]):
    return flatten(map(mapper, input_stream))


def run_reduce[T, U](reducer: Callable[Iterable[T], U],
                     input_stream: Iterable[T],
                     key: [str]) -> Iterable[U]:
    def key_func(item):
        return tuple(getattr(item, k) for k in key)

    sorted_stream = sorted(input_stream, key=key_func)
    grouped_stream = groupby(sorted_stream, key=key_func)
    return flatten(map(lambda x: reducer(x[1]), grouped_stream))


class SimpleMapReduce:
    def __init__(self, stream):
        self._stream = stream

    def map(self, mapper):
        self._stream = run_map(mapper, self._stream)
        return self

    def reduce(self, reducer, key):
        self._stream = run_reduce(reducer, self._stream, key)
        return self

    def output(self):
        return self._stream