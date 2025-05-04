from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("StreamingProcessingExample").getOrCreate()

schema = StructType([
    StructField("id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("value", StringType(), True)
])

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('BatchProcessingExample').getOrCreate()
df_batch = spark.readStream.option('header', True).option('maxFilesPerTrigger',
    '1').schema(schema).csv('stream_input/')
df_filtered = df_batch.filter(df_batch['value'] > 50)
df_grouped = df_filtered.groupBy('category').count()
query = df_grouped.writeStream.outputMode('complete').format('console').start()

query.awaitTermination()
