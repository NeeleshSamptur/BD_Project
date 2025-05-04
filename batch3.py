from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_

spark = SparkSession.builder.appName("DFReduceLike").getOrCreate()

df = spark.read.option("header", True).csv("batch_input/data.csv")

df_cast = df.select(
    col("category"),
    col("value").cast("double").alias("value")
)

df_grouped = df_cast.groupBy("category").agg(sum_("value").alias("total_value"))
df_grouped.write.csv("batch_output/")
# df_grouped.show()