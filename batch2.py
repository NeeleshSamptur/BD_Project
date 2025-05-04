from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, window, col, sum as sum_

spark = SparkSession.builder.appName("BatchWindowAgg").getOrCreate()
df = spark.read.option("header", True).csv("batch_input/data.csv")

df = df.withColumn("timestamp", to_timestamp("timestamp"))
df = df.select("timestamp", "category", col("value").cast("double").alias("value"))

df_windowed = df.groupBy(
    window("timestamp", "5 minutes"), "category"
).agg(sum_("value").alias("total_value"))

df_windowed.write.csv("batch_output/")