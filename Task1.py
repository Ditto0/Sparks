import pyspark
from pyspark import SparkConf, SparkContext
import sys
import os
os.environ['SPARK_HOME'] = "D:\spark-2.4.3-bin-hadoop2.7"
sys.path.append("D:\spark-2.4.3-bin-hadoop2.7\python")
sys.path.append("D:\spark-2.4.3-bin-hadoop2.7\python\lib")
sc = pyspark.SparkContext( 'local', 'test')


logFile = "test.txt"
text = sc.textFile(logFile)
print(text)
words = text.flatMap(lambda line:line.split(" "))
counts = words.map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y)

result=counts.collect()

print(result)
for (word,count) in result:

    print(word,count)
#numAs = logData.filter(lambda line: 'a' in line).count()
#count = word.map(lambda word: (a, 1)).reduceByKey(lambda x, y: x + y)
#count.foreach(print)
#print(numAs+"123")
#numBs = logData.filter(lambda line: 'b' in line).count()

#print('Lines with a: %s, Lines with b: %s' % (numAs, numBs))