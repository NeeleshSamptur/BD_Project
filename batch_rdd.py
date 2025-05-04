# Batch RDD word count
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

spark = SparkSession.builder.appName("GroupBy").getOrCreate()
lines_rdd = spark.sparkContext.textFile("path/to/input.txt")
word_counts_rdd = (lines_rdd
    .flatMap(lambda line: line.split(" "))             
    .map(lambda word: (word, 1))                         
    .reduceByKey(lambda a, b: a + b))                  
result = word_counts_rdd.collect() 
