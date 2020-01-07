from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Finding Avg of num of friends with Age")
sc = SparkContext(conf=conf)

def parseAge(lines):
    fields = lines.split(',')
    age = int(fields[2])
    frnds = int(fields[3])
    return(age,frnds)

lines = sc.textFile("file:///E:/Study/SparkCourse/data/fakefriends.csv")

ageByFriends = lines.map(parseAge)

totalsByAge = ageByFriends.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1]))

avgByAge = totalsByAge.mapValues(lambda x: x[0]/x[1])

for result in avgByAge.collect():
    print(result)