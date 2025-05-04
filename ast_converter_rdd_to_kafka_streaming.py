import ast
import astor
import argparse
import re


class RDDToKafkaStreamTransformer(ast.NodeTransformer):
    def __init__(self):
        super().__init__()
        self.rdd_variable = None
        self.df_lines_name = None
        self.df_pairs_name = None
        self.df_grouped_name = None
        self.saw_map_to_kv = False
        self.output_variable = None

    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call):
            func = node.value.func
            if isinstance(func, ast.Attribute) and func.attr in {"collect", "show", "take"}:
                print(f"⚠️ Removing unsupported action: .{func.attr}()")
                return None
            if isinstance(func, ast.Name) and func.id == "print":
                print("⚠️ Removing print() statement")
                return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        if isinstance(node.value, ast.Call):
            func = node.value.func

            # Detect textFile → replace with Kafka streaming reader
            if isinstance(func, ast.Attribute) and func.attr == "textFile":
                self.rdd_variable = node.targets[0].id
                self.df_lines_name = self.rdd_variable
                print("🔄 Converting sc.textFile → spark.readStream from Kafka")
                return ast.parse(f"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, split, col, sum
spark = SparkSession.builder \\
    .appName("pyspark-kafka-streaming") \\
    .master("spark://spark-master:7077") \\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \\
    .config("spark.executor.memory", "512m") \\
    .getOrCreate()

df_streamed_raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9093")
    .option("subscribe", "topic_test")
    .option("startingOffsets", "earliest")
    .load())

{self.df_lines_name} = df_streamed_raw.selectExpr("CAST(value AS STRING) as line")
                """).body

            # Detect map that splits line (CSV)
            if isinstance(func, ast.Attribute) and func.attr == "map":
                if isinstance(node.targets[0], ast.Name):
                    current_var = node.targets[0].id
                    if not self.df_pairs_name:
                        self.df_pairs_name = current_var
                        print("🔄 Converting map → split CSV line into category and amount")
                        return ast.parse(f"""{current_var} = {self.df_lines_name}.select(
    split(col("line"), ",")[0].alias("category"),
    split(col("line"), ",")[1].cast("double").alias("amount"))""").body

            # Detect reduceByKey → groupBy().agg(sum())
            if isinstance(func, ast.Attribute) and func.attr == "reduceByKey":
                if isinstance(node.targets[0], ast.Name):
                    self.df_grouped_name = node.targets[0].id
                    print("🔄 Converting reduceByKey → groupBy + sum")
                    return ast.parse(f"""{self.df_grouped_name} = {self.df_pairs_name}.groupBy("category").agg(sum("amount").alias("total_amount"))""").body

            # Replace .collect() assignment with writeStream
            if isinstance(func, ast.Attribute) and func.attr == "collect":
                print("🔄 Replacing .collect() with writeStream logic")
                return ast.parse(f"""
query = {self.df_grouped_name}.writeStream \\
    .format("console") \\
    .outputMode("complete") \\
    .start()
query.awaitTermination()
                """).body

        return self.generic_visit(node)

    def visit_Module(self, node):
        node.body = [stmt for stmt in node.body if stmt is not None]
        return self.generic_visit(node)


def preprocess_batch_code(code):
    code = re.sub(r"from pyspark\.sql import SparkSession\n?", "", code)
    code = re.sub(r"from pyspark\.sql\.functions import [^\n]+\n?", "", code)
    code = re.sub(r"spark\s*=\s*SparkSession\.builder[^\n]+getOrCreate\(\)", "", code)
    return code.strip()


def convert_batch_rdd_to_stream_kafka(batch_file_path: str, output_file_path: str):
    with open(batch_file_path, 'r') as f:
        batch_code = f.read()

    batch_code = preprocess_batch_code(batch_code)
    tree = ast.parse(batch_code)
    transformer = RDDToKafkaStreamTransformer()
    transformed_tree = transformer.visit(tree)
    transformed_tree = ast.fix_missing_locations(transformed_tree)

    stream_code = astor.to_source(transformed_tree)
    with open(output_file_path, "w", encoding="utf-8") as f:
        f.write(stream_code)

    print(f"Kafka-based streaming code written to {output_file_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert RDD-based PySpark code to Kafka-based Structured Streaming")
    parser.add_argument("input_file", help="Path to batch RDD Python file")
    parser.add_argument("output_file", help="Path to save the converted streaming code")
    args = parser.parse_args()
    convert_batch_rdd_to_stream_kafka(args.input_file, args.output_file)
