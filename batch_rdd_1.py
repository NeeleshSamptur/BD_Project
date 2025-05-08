from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MaxPerKey").getOrCreate()

lines = spark.sparkContext.textFile("values.csv")
pairs = lines.map(lambda line: line.split(",")) \
             .map(lambda fields: (fields[0], int(fields[1])))
max_result = pairs.reduceByKey(lambda a, b: max(a, b))
max_result.collect()
