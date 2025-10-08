from simplemr import SimpleMapReduce
from task_3 import process

with open("log.tsv", "r") as input_stream:
    mrjob = process(SimpleMapReduce(input_stream))
    for item in mrjob.output():
        print(item)
