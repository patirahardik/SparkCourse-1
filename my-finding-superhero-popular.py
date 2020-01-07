from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Finding super hero")
sc = SparkContext(conf=conf)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf-8"))

def countCoOccurance(line):
    fields = line.split()
    return (int(fields[0]), len(fields) - 1)


names = sc.textFile("file:///C:/Users/yyyyy/Desktop/git_projects/SparkCourse/data/Marvel-Names.txt")
namesRDD = names.map(parseNames)


lines = sc.textFile("file:///C:/Users/yyyyy/Desktop/git_projects/SparkCourse/data/Marvel-Graph.txt")
pairing = lines.map(countCoOccurance)

totalFriendsByCharater = pairing.reduceByKey(lambda x,y : x + y)
flipped = totalFriendsByCharater.map(lambda x: (x[1],x[0]))

mostpopular = flipped.max()

mostPopularName = namesRDD.lookup(mostpopular[1])[0]

print( str(mostPopularName.decode("utf-8")) + " is the most popular hero, with " + str(mostpopular[0]) + " co-appearance")