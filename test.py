import pyspark
from pyspark import SparkConf, SparkContext

sc = pyspark.SparkContext( 'local', 'test')

print(123455)
logFile = "E:/Python/Sparks/edd.txt"
logData = sc.textFile(logFile, 2).cache()
numAs = logData.filter(lambda line: 'a' in line).count()
numBs = logData.filter(lambda line: 'b' in line).count()

print('Lines with a: %s, Lines with b: %s' % (numAs, numBs))