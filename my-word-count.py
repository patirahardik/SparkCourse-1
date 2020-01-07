from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("My Word Count")
sc = SparkContext(conf=conf)

def normalizedWord(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


lines = sc.textFile("file:///E:/Study/SparkCourse/data/Book.txt")

flatMapLines = lines.flatMap(normalizedWord)
wordCount = flatMapLines.countByValue()


for word, count in wordCount.items():
        print(word, count)

