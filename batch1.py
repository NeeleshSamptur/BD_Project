from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

spark = SparkSession.builder.appName("GroupByMax").getOrCreate()
df = spark.read.option("header", True).csv("batch_input/data.csv")

df_cast = df.select(col("category"), col("value").cast("double"))
df_agg = df_cast.groupBy("category").agg(max("value").alias("max_val"))

df_agg.write.csv("batch_output/")
# df_agg.show()