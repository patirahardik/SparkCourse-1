from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My-Minimum-Temprature-Code")
sc = SparkContext(conf=conf)

def Parsed_Fnc(line):
    fields = line.split(',')
    stationId = fields[0]
    entryType = fields[2]
    temprature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationId,entryType,temprature)

data = sc.textFile("file:///E:/Study/SparkCourse/data/1800.csv")

dataParse = data.map(Parsed_Fnc)

dataMinTemp = dataParse.filter(lambda x: "TMIN" in x[1])

minTempByStation = dataMinTemp.map(lambda x: (x[0],x[2])).reduceByKey(lambda x,y: min(x,y))

for result in minTempByStation.collect():
    print (result)