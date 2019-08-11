from pyspark import SparkContext

sc = SparkContext("local","PatRank")
lines = sc.textFile("PAT.txt")


r = lines.map(lambda line:line.split(" ")).map(lambda line:((line[0],line[1]),line[2]))
pairRDD = r.reduceByKey(lambda a,b:a if a>b  else b)
#id�ͷ�����һ��RDD
pairRDD = pairRDD.map(lambda line:(line[0][0],line[1]))
#����ÿ��id���ܷ�
SumVal = pairRDD.reduceByKey(lambda a,b:int(a)+int(b) if b!="-1" else int(a)+0)
SumVal.mapValues(lambda a,b:a>b)
#����ͬkey��id���з���
EachVal = pairRDD.groupByKey()#.map(lambda line:(line[0],list(line[1]))).collect()
#���ֺܷ�ÿ����ĵ÷ֺϲ�������list��ÿһ��ķ����������
zf = SumVal.join(EachVal).map(lambda line:(line[0],line[1][0],list(line[1][1]))).collect()
for i,j,val in zf:
    print(i,j,val)
#r = lines.map(lambda line:((line[0],line[1]),line[2])).reduceByKey(lambda a,b:(a>b and a or b))

