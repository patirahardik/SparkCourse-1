from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My Assignment 1")
sc = SparkContext(conf=conf)

def parsedData(line):
    fields = line.split(',')
    custID = int(fields[0])
    spent = float(fields[2])
    return (custID, spent)

data = sc.textFile("file:///E:/Study/SparkCourse/data/customer-orders.csv")

parsedMapData = data.map(parsedData)

totalSpent = parsedMapData.reduceByKey(lambda x,y: x + y).map(lambda x: (x[1],x[0])).sortByKey()

result = totalSpent.collect()

for i in result:
    print(i)