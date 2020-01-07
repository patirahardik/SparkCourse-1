from pyspark import SparkConf, SparkContext

def loadMoviesItems():
    movieName={}
    with open("E:/Study/SparkCourse/ml-100k/u.item") as f:
        for line in f:
            fields = line.split("|")
            movieName[int(fields[0])]=fields[1]
    
    return movieName

conf = SparkConf().setMaster("local").setAppName("My Most Popular Movie")
sc = SparkContext(conf=conf)

movieItems= sc.broadcast(loadMoviesItems())


data = sc.textFile("file:///E:/Study/SparkCourse/ml-100k/u.data")

mostPopularMovie = data.map(lambda x: x.split()[1]).map(lambda x: (int(x),1)).reduceByKey(lambda x,y: x + y)


mostPopularMovieSort = mostPopularMovie.map(lambda x : (x[1],x[0])).sortByKey()

mostPopularMovieName = mostPopularMovieSort.map(lambda x: (movieItems.value[x[1]], x[0]))

for i in mostPopularMovieName.collect():
    print(i)
