from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, explode, split
spark = SparkSession.builder.appName('pyspark-kafka-streaming').master(
    'spark://spark-master:7077').config('spark.jars.packages',
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0').config(
    'spark.executor.memory', '512m').getOrCreate()
df_streamed_raw = spark.readStream.format('kafka').option(
    'kafka.bootstrap.servers', 'kafka:9093').option('subscribe', 'topic_test'
    ).option('startingOffsets', 'earliest').load()
df_lines = df_streamed_raw.selectExpr('CAST(value AS STRING) as line')
df_words = df_lines.select(explode(split(df_lines.line, ' ')).alias('word'))
df_counts = df_words.groupBy('word').count()
query = df_counts.writeStream.format('console').outputMode('complete').start()
query.awaitTermination()
