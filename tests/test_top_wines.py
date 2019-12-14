# Find the best rated wines

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SQLProject")
sc = SparkContext(conf = conf)


def parseLine(line):
    fields = line.split(';')
    designation = str(fields[2])
    points = float(fields[3])
    return designation, points


lines = sc.textFile("file:///Users/luisguillermo/IE/Spark/Final Project/wine-reviews/10RecordsTest.csv")

rdd = lines.map(parseLine)

best_wines_designation = rdd.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

results = best_wines_designation.collect()

for result in results:
    print result
