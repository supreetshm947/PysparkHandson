from pyspark.sql import SparkSession
from operator import add

spark = SparkSession.builder.appName('Word Count').getOrCreate()

file = spark.read.text("./data/text.txt")
words = file.rdd.flatMap(lambda line: line.value.split())
words = words.filter(lambda word: word not in [",","."])

word_count = words.map(lambda x: (x,1))

word_counts = sorted(word_count.reduceByKey(add).collect())


for word, count in word_counts:
    print(word + ": " + str(count))

