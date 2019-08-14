from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()
studentRDD = spark.sparkContext.parallelize(["3 Rongcheng M 26","4 Guanhua M 27"]).map(lambda line : line.split(" "))
schema = StructType([StructField("name", StringType(), True),StructField("gender", StringType(), True),StructField("age",IntegerType(), True)])
rowRDD = studentRDD.map(lambda p : Row(p[1].strip(), p[2].strip(),int(p[3])))
studentDF = spark.createDataFrame(rowRDD,schema)
studentDF.show()
prop={}
prop['user'] = 'root'
prop['password'] = 'hadoop'
prop['driver'] = "com.mysql.jdbc.Driver"
studentDF.write.jdbc("jdbc:mysql://localhost:3306/spark","student","append",prop)
