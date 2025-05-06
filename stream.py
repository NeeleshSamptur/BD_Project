from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('BatchToStream').getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc, 5)
lines = ssc.textFileStream('stream_input/')
lines.foreachRDD(lambda rdd: rdd.map(lambda line: line.split(',')).filter(
    lambda fields: int(fields[2]) > 50).map(lambda fields: (fields[1], 1)).
    reduceByKey(lambda x, y: x + y).foreach(lambda result: print(result)))
ssc.start()
ssc.awaitTermination()
