import pyspark
from pyspark import SparkConf, SparkContext
import sys
import os
os.environ['SPARK_HOME'] = "D:\spark-2.4.3-bin-hadoop2.7"
sys.path.append("D:\spark-2.4.3-bin-hadoop2.7\python")
sys.path.append("D:\spark-2.4.3-bin-hadoop2.7\python\lib")
sc = pyspark.SparkContext( 'local', 'test')

print(123455)
logFile = "E:/Python/Sparks/edd.txt"
logData = sc.textFile(logFile, 2).cache()
numAs = logData.filter(lambda line: 'a' in line).count()
numBs = logData.filter(lambda line: 'b' in line).count()

print('Lines with a: %s, Lines with b: %s' % (numAs, numBs))