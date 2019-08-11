import os
import shutil
from pyspark import SparkContext



sc = SparkContext("local","PatRank")
lines = sc.textFile("PAT.txt")

r = lines.flatMap(lambda line:((line[0],line[1]),line[2])).reduceByKey(max)
r.foreach(print)
'''
sc = SparkContext("local","wordcount")

lines = sc.textFile("test.txt")
con = lines.count()
print(con)
f = lines.collect()
print(f)

hellolines = lines.filter(lambda line: "hello" in line)
hello_count = hellolines.first()
print(hello_count)

maxNum = lines.map(lambda line: len(line.split(" ")))
maxNum.foreach(print)
'''
