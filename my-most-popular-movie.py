from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My Most Popular Movie")
sc = SparkContext(conf=conf)

data = sc.textFile("file:///E:/Study/SparkCourse/ml-100k/u.data")

mostPopularMovie = data.map(lambda x: x.split()[1]).map(lambda x: (x,1)).reduceByKey(lambda x,y: x + y)


mostPopularMovieSort = mostPopularMovie.map(lambda x : (x[1],x[0])).sortByKey().top(1)

print(mostPopularMovieSort)