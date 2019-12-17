# Find which countries have the most wines

from pyspark import SparkConf, SparkContext
import matplotlib.pyplot as plt

conf = SparkConf().setMaster("local").setAppName("SQLProject")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(';')
    country = str(fields[1])
    return country, 1


lines = sc.textFile("file:///Users/luisguillermo/IE/Spark/Final Project/wine-reviews/10RecordsTest.csv")

rdd = lines.map(parseLine)

rdd_reduced = rdd.reduceByKey(lambda x, y: x + y)

inverted_sorted = rdd_reduced.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

final = inverted_sorted.map(lambda x: (x[1], x[0]))

results = final.collect()

for result in results:
    print result

plt.bar(results, 20)
plt.ylabel("Countries")
plt.show()
