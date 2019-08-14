from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

def Create_DF():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.json("people.json")
    df.show()
    df.printSchema()
    df.select(df.name,df.age).show()
    df.filter(df.age > 20).show()
    df.groupBy("age").count().show()
    df.sort(df.age.desc()).show()
    df.select(df.name.alias("username"),df.age).show()

def f(x):
    rel = {}
    rel["name"] = x[0]
    rel["age"] = x[1]
    return rel

def Trans_DF1():
    spark = SparkSession.builder.getOrCreate()
    sc = SparkContext.getOrCreate()
    peopleDF = sc.textFile("people.txt")
    peopleDF = peopleDF.map(lambda line:line.split(",")).map(lambda x:Row(**f(x))).toDF()
    peopleDF.createOrReplaceTempView("people")
    personDF = spark.sql("select * from people")
    personDF.rdd.map(lambda t: "Name:" + t[0] + "," + "Age:" + t[1]).foreach(print)
    return personDF
def Trans_DF2():
    spark = SparkSession.builder.getOrCreate()
    sc = SparkContext.getOrCreate()
    peopleRDD = sc.textFile("people.txt")
    schemaString = "name age"
    fields = list(map(lambda fieldName: StructField(fieldName, StringType(), nullable=True), schemaString.split(" ")))
    schema = StructType(fields)
    rowRDD = peopleRDD.map(lambda line:line.split(",")).map(lambda attributes : Row(attributes[0], attributes[1]))
    peopleDF = spark.createDataFrame(rowRDD,schema)
    peopleDF.createTempView("people")
    results = spark.sql("select * from people")
    results.rdd.map(lambda attributes: "name: " + attributes[0] + "," + "age:" + attributes[1]).foreach(print)
    return results
def Save_RDD1():
    spark = SparkSession.builder.getOrCreate()
    peopleDF = spark.read.format("json").load("people.json")
    peopleDF.select("name","age").write.format("csv").save("newpeople.csv")
def Save_RDD2():
    spark = SparkSession.builder.getOrCreate()
    peopleDF = spark.read.format("json").load("people.json")
    peopleDF.rdd.saveAsTextFile("newpeople.txt")
    pass

if __name__=="__main__":
    """
    Create_DF()
    Trans_DF1()
    Trans_DF2()
    Save_RDD1()
    """
    Save_RDD2()