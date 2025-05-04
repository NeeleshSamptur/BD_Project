# Batch RDD filtering example
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

spark = SparkSession.builder.appName("LogFiltering").getOrCreate()

logs_rdd = spark.sparkContext.textFile("path/to/logs.txt")
error_lines_rdd = logs_rdd.filter(lambda line: "ERROR" in line)      # filter lines containing "ERROR"
# Suppose log format: timestamp,level,message (CSV)
error_messages_rdd = error_lines_rdd.map(lambda line: line.split(",")[2])  # extract the 'message' field
error_lines_rdd.collect()  # collect the filtered lines
