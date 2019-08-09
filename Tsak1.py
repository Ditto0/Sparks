import findspark
import pyspark
#可在环境变量中进行设置，即PATH中加入如下地址
findspark.init("D:\spark-2.4.3-bin-hadoop2.7")
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf

# 创建sc
sc=SparkContext("local","Simple")