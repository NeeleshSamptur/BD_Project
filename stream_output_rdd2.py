from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, split, col, sum
spark = SparkSession.builder.appName('pyspark-kafka-streaming').master(
    'spark://spark-master:7077').config('spark.jars.packages',
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0').config(
    'spark.executor.memory', '512m').getOrCreate()
df_streamed_raw = spark.readStream.format('kafka').option(
    'kafka.bootstrap.servers', 'kafka:9093').option('subscribe', 'topic_test'
    ).option('startingOffsets', 'earliest').load()
data_rdd = df_streamed_raw.selectExpr('CAST(value AS STRING) as line')
pairs_rdd = data_rdd.select(split(col('line'), ',')[0].alias('category'),
    split(col('line'), ',')[1].cast('double').alias('amount'))
key_value_rdd = pairs_rdd.map(lambda fields: (fields[0], float(fields[1])))
summed_rdd = pairs_rdd.groupBy('category').agg(sum('amount').alias(
    'total_amount'))
