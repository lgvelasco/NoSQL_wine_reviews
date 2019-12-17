# Average price of a wine bottle at a specific point

from pyspark import SparkConf, SparkContext
import matplotlib.pyplot as plt

conf = SparkConf().setMaster("local").setAppName("SQLProject")
sc = SparkContext(conf = conf)


def parseLine(line):
    fields = line.split(',')
    try:
        points = float(fields[3])
    except:
        points = 0

    try:
        price = float(fields[4])
    except:
        price = 0
    return points, price


lines = sc.textFile("file:///Users/luisguillermo/IE/Spark/Final Project/wine-reviews/150Wines2.txt")
rdd = lines.map(parseLine)

temp = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_points = temp.mapValues(lambda x: x[0] / x[1]).sortByKey(ascending=False)

results = average_points.take(10)

for result in results:
    # print("POINTS: " + str(result[0]) + "   PRICE: " + str(result[1]))
    print(result)
