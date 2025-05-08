from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, split, col, sum, max, min, explode, lit
spark = SparkSession.builder.appName('pyspark-kafka-streaming').master(
    'spark://spark-master:7077').config('spark.jars.packages',
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0').config(
    'spark.executor.memory', '512m').getOrCreate()
df_streamed_raw = spark.readStream.format('kafka').option(
    'kafka.bootstrap.servers', 'kafka:9093').option('subscribe', 'topic_test'
    ).option('startingOffsets', 'earliest').load()
lines_rdd = df_streamed_raw.selectExpr('CAST(value AS STRING) as line')
pairs = lines_rdd.select(split(col('line'), ',')[0].alias('key'), split(col
    ('line'), ',')[1].cast('double').alias('value'))
max_result = pairs.groupBy('key').agg(max('value').alias('aggregated'))
query = max_result.writeStream.format('console').outputMode('complete').start()
query.awaitTermination()
