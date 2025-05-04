# Batch RDD filtering example
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

spark = SparkSession.builder.appName("BatchAggregation").getOrCreate()

# Batch RDD aggregation example
data_rdd = spark.sparkContext.textFile("path/to/data.csv")
# Each line is like "Category,Amount"
pairs_rdd = data_rdd.map(lambda line: line.split(","))              # split CSV fields
key_value_rdd = pairs_rdd.map(lambda fields: (fields[0], float(fields[1])))  # (category, amount)
summed_rdd = key_value_rdd.reduceByKey(lambda a, b: a + b)          # sum amounts per category
summed_rdd.collect()