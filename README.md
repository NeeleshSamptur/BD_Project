# Batch RDD-to-Kafka Structured Streaming Code Converter 

This tool automatically converts PySpark batch RDD-based code into equivalent Kafka-based structured streaming DataFrame code using Python's `ast` module.


## Features

- Converts `sc.textFile(...)` to `spark.readStream` with Kafka source.
- Transforms chained `.flatMap().map().reduceByKey()` to equivalent logic
- Automatically identifies aggregation functions (e.g., `sum`, `max`, `min`)
- Replaces `.collect()` or `.show()` with `writeStream`
- Dynamically handles various `lambda` map and reduce patterns

---

## Directory Structure

```
├── ast_converter_rdd_to_kafka_streaming.py # Main interface
├── batch_rdd.py # Example batch RDD input
├── stream_output_rdd.py # Output structured streaming version for batch_rdd.py
├── batch_rdd_1.py # Example batch RDD input
├── stream_output_rdd1.py # Output structured streaming version batch_rdd_1.py
├── ast_converter #converts df based batch code to stream code 
└── README.md
```

## Run the converter:

```bash
python ast_converter_rdd_to_kafka_streaming.py batch_rdd.py stream_output_rdd.py

```
