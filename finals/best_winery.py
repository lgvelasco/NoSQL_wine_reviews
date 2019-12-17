# Find the best winery based on average points

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SQLProject")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    designation = (fields[2])
    try:
        points = float(fields[3])
    except:
        points = 0
    return designation, points


lines = sc.textFile("file:///Users/luisguillermo/IE/Spark/Final Project/wine-reviews/150Wines2.txt")

rdd = lines.map(parseLine)

totalsByCountry = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

averageByCountry = totalsByCountry.mapValues(lambda x: x[0] / x[1])

sorted_inverted_average_country = averageByCountry.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

results = sorted_inverted_average_country.take(10)

for result in results:
    print result
