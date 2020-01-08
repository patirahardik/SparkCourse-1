#importing pyspark libraries
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My Degree of Separation")
sc = SparkContext(conf=conf)

hitCounter=sc.accumulator(0)

startCharaterID=2708 
targetCharacterID=2111 

# created to convert data in BFS format
def convertToBFS(line):
    fields = line.split()
    charaterID = int(fields[0])
    
    connections = []
    for i in fields[1:]:
        connections.append(int(i))
    distance = 9999
    color = 'WHITE'
    if (charaterID == startCharaterID):
        distance = 0
        color = 'GRAY'
    return (charaterID, (connections, distance, color))

#Mapping BFS for iteration
def bfsMap(data):
    charID = data[0]
    connection = data[1][0]
    dist = data[1][1]
    color = data[1][2]
    result = []
    if(color == 'GRAY'):
        for conn in connection:
            newCharID = conn
            newDist = dist + 1
            newConn = []
            newColor = 'GRAY'

            if (targetCharacterID == newCharID):
                hitCounter.add(newDist)

            result.append((newCharID, (newConn, newDist, newColor)))
            color = 'BLACK'
    result.append((charID, (connection, dist, color)))
    return result

def bfsReduce(data1, data2):
    conn1 = data1[0]
    conn2 = data2[0]
    conn=[]
    dist1=data1[1]
    dist2=data2[1]
    color1=data1[2]
    color2=data2[2]
    color=color1
    distance = 9999
    if (len(conn1) > 0):
        conn.extend(conn1)
    if (len(conn2) > 0):
        conn.extend(conn2)
    if (dist1 < distance):
        distance = dist1    
    if (dist2 < distance):
        distance = dist2
        # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2
    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2
    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1
    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1
    return (conn, distance, color)




lines = sc.textFile("file:///C:/Users/yyyyy/Desktop/git_projects/SparkCourse/data/Marvel-Graph.txt")
dataBFS = lines.map(convertToBFS)





for i in range(1000):

    print("Running Iteration " + str(i + 1))

    flatdataBFS = dataBFS.flatMap(bfsMap)

    print("Processing " + str(flatdataBFS.count()) + " values.")

    
    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
            + " different direction(s).")
        break

    dataBFS=flatdataBFS.reduceByKey(bfsReduce)
        