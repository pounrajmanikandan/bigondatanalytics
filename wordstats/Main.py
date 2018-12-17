from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('WordStatistics')
sc = SparkContext(conf=conf)

text_file = sc.textFile("hdfs:///tmp/data/shakespeare.txt")

counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

print ("Number of elements: " + str(counts.count()))
counts.saveAsTextFile("hdfs:///tmp/data/shakespeareoutput")