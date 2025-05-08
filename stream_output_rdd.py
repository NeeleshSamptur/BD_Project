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
lines_rdd = lines_rdd.select(explode(split(lines_rdd.line, ' ')).alias('word'))
word_counts_rdd = lines_rdd.withColumn('value', lit(1)).withColumnRenamed(
    'word', 'key')
word_counts_rdd = word_counts_rdd.groupBy('key').agg(sum('value').alias(
    'aggregated'))
query = word_counts_rdd.writeStream.format('console').outputMode('complete'
    ).start()
query.awaitTermination()
