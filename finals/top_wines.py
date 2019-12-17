# Find the best rated wines

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SQLProject")
sc = SparkContext(conf = conf)


def parseLine(line):
    fields = line.split(',')
    designation = (fields[2])
    points = float(fields[3])
    variety = fields[9]
    try:
        price = float(fields[4])
    except:
        price = 0

    return designation, points, variety, price


lines = sc.textFile("file:///Users/luisguillermo/IE/Spark/Final Project/wine-reviews/150Wines2.txt")

rdd = lines.map(parseLine)

best_wines_designation = rdd.map(lambda x: (x[1], x[0], x[2], x[3])).sortByKey(ascending=False)

results = best_wines_designation.take(10)

for result in results:
    print result
