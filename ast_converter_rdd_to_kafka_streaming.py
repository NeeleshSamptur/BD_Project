import ast
import astor
import argparse
import re

class RDDToKafkaStreamTransformer(ast.NodeTransformer):
    def __init__(self):
        super().__init__()
        self.collected_df_name = None
        self.df_pairs_name = None
        self.df_counts_name = None
        self.has_textfile = False

    def extract_lambda(self, node):
        try:
            return ast.unparse(node)
        except Exception:
            return "<lambda>"

    def process_chain(self, call_node, target_name, full_node=None):
        if not isinstance(call_node, ast.Call):
            return None

        ops = []
        current = call_node
        while isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
            ops.append((current.func.attr, current.args))
            current = current.func.value

        ops.reverse()
        df_name = "lines_rdd"
        code_snippets = []

        for i, (op, args) in enumerate(ops):
            if op == "flatMap":
                lambda_expr = ast.unparse(args[0]) if args else ""
                print("Captured flatMap:", lambda_expr)
                snippet = f"{df_name} = {df_name}.select(explode(split({df_name}.line, ' ')).alias('word'))"
                code_snippets.append(snippet)

            elif op == "filter":
                lambda_expr = ast.unparse(args[0]) if args else ""
                print("Captured filter:", lambda_expr)
                snippet = f"{df_name} = {df_name}.filter({lambda_expr})"
                code_snippets.append(snippet)

            elif op == "map":
                lambda_expr = self.extract_lambda(args[0])
                print("Captured map:", lambda_expr)
                if lambda_expr == "lambda word: (word, 1)":
                    snippet = f"{target_name} = {df_name}.withColumn('value', lit(1)).withColumnRenamed('word', 'key')"
                    df_name = target_name
                    self.df_pairs_name = df_name
                    code_snippets.append(snippet)
                elif "split" in lambda_expr and "," in lambda_expr:
                    snippet = f"""{target_name} = {df_name}.select(
    split(col("line"), ",")[0].alias("key"),
    split(col("line"), ",")[1].cast("double").alias("value"))"""
                    df_name = target_name
                    self.df_pairs_name = df_name
                    code_snippets.append(snippet)
                else:
                    code_snippets.append(f"# Unsupported map lambda: {lambda_expr}")

            elif op == "reduceByKey":
                self.reduceby_lambda = self.extract_lambda(args[0])
                print(f"Captured reduceByKey: {self.reduceby_lambda}")
                self.df_counts_name = target_name

                if not self.df_pairs_name:
                    print("No preceding map() found. Using default key/value structure.")
                    code_snippets.append(f"# Cannot convert reduceByKey without key-value context")
                else:
                    agg_func = "sum"  # Default
                    # handle different lambda expressions
                    if "+" in self.reduceby_lambda:
                        agg_func = "sum"
                    elif "max(a, b)" in self.reduceby_lambda or "b if b > a" in self.reduceby_lambda:
                        agg_func = "max"
                    elif "min(a, b)" in self.reduceby_lambda or "b if b < a" in self.reduceby_lambda:
                        agg_func = "min"

                    snippet = f"""{target_name} = {self.df_pairs_name}.groupBy("key").agg({agg_func}("value").alias("aggregated"))"""
                    code_snippets.append(snippet)

        if code_snippets:
            return ast.parse("\n".join(code_snippets)).body

        return self.generic_visit(full_node) if full_node else None

    def visit_Assign(self, node):
        if isinstance(node.value, ast.Call):
            target = node.targets[0]
            if isinstance(target, ast.Name):
                target_name = target.id
            else:
                return self.generic_visit(node)

            func = node.value.func
            if isinstance(func, ast.Attribute) and func.attr == "textFile":
                print("Converting sc.textFile → Kafka stream reader")
                self.has_textfile = True
                return ast.parse(f"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, split, col, sum, max, min, explode, lit
spark = SparkSession.builder \\
    .appName("pyspark-kafka-streaming") \\
    .master("spark://spark-master:7077") \\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \\
    .config("spark.executor.memory", "512m") \\
    .getOrCreate()

df_streamed_raw = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers", "kafka:9093") \\
    .option("subscribe", "topic_test") \\
    .option("startingOffsets", "earliest") \\
    .load()
lines_rdd = df_streamed_raw.selectExpr("CAST(value AS STRING) as line")
                """).body
            if isinstance(func, ast.Attribute) and func.attr == "collect":
                print("Replacing .collect() assignment with writeStream logic")
                self.collected_df_name = func.value.id if isinstance(func.value, ast.Name) else "UNKNOWN_DF"
                return ast.parse(f"""
query = {self.collected_df_name}.writeStream \\
    .format("console") \\
    .outputMode("complete") \\
    .start()
query.awaitTermination()
            """).body
            
            chain_result = self.process_chain(node.value, target_name, node)
            if chain_result:
                return chain_result

        return self.generic_visit(node)

    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call):
            func = node.value.func
            if isinstance(func, ast.Attribute) and func.attr in {"collect", "show", "take"}:
                print(f"Replacing .{func.attr}() with writeStream logic")
                if self.df_counts_name:
                    return ast.parse(f"""
query = {self.df_counts_name}.writeStream \\
    .format("console") \\
    .outputMode("complete") \\
    .start()
query.awaitTermination()""").body
        return self.generic_visit(node)

    def visit_Module(self, node):
        new_body = []
        for stmt in node.body:
            result = self.visit(stmt)
            if result is None:
                continue
            elif isinstance(result, list):
                new_body.extend(result)
            else:
                new_body.append(result)
        node.body = new_body
        return node

def preprocess_batch_code(code):
    code = re.sub(r"from pyspark\.sql import SparkSession\n?", "", code)
    code = re.sub(r"from pyspark\.sql\.functions import [^\n]+\n?", "", code)
    code = re.sub(r"spark\s*=\s*SparkSession\.builder[^\n]+getOrCreate\(\)", "", code)
    return code.strip()

def convert_batch_rdd_to_stream_kafka(batch_file_path: str, output_file_path: str):
    with open(batch_file_path, 'r', encoding='utf-8') as f:
        batch_code = f.read()
    batch_code = preprocess_batch_code(batch_code)
    tree = ast.parse(batch_code)
    transformer = RDDToKafkaStreamTransformer()
    transformed_tree = transformer.visit(tree)
    transformed_tree = ast.fix_missing_locations(transformed_tree)
    stream_code = astor.to_source(transformed_tree)
    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write(stream_code)
    print(f"Kafka-based streaming code written to {output_file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert RDD-based PySpark code to Kafka-based Structured Streaming")
    parser.add_argument("input_file", help="Path to batch RDD Python file")
    parser.add_argument("output_file", help="Path to save the converted streaming code")
    args = parser.parse_args()
    convert_batch_rdd_to_stream_kafka(args.input_file, args.output_file)
