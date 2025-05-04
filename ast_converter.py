# import argparse

# # Step 1: Parse command-line argument
# parser = argparse.ArgumentParser(description="Convert batch Spark code to streaming Spark code.")
# parser.add_argument("input_file", help="Path to batch Python file (batch.py)")
# args = parser.parse_args()

# # Step 2: Read batch.py
# with open(args.input_file, "r") as file:
#     batch_code = file.read()

# # Step 3: Build fixed stream.py
# stream_py_code = """
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# # Initialize Spark session
# spark = SparkSession.builder.appName("StreamingProcessingExample").getOrCreate()

# # Define schema
# schema = StructType([
#     StructField("id", StringType(), True),
#     StructField("category", StringType(), True),
#     StructField("value", StringType(), True)
# ])

# # Read streaming data
# df_stream_raw = spark.readStream.option("header", True).option("maxFilesPerTrigger", 1).schema(schema).csv("stream_input/")

# # Select and cast
# df_stream = df_stream_raw.select(
#     col("category"),
#     col("value").cast("double")
# )

# # Apply transformations
# df_filtered = df_stream.filter(col('value') > 50)
# df_grouped = df_filtered.groupBy("category").count()

# # Write output to console
# query = df_grouped.writeStream.outputMode("complete").format("console").start()
# query.awaitTermination()
# """

# # Step 4: Save stream.py
# with open("stream.py", "w") as file:
#     file.write(stream_py_code)

# print("✅ Correct stream.py generated successfully!")




import ast
import astor
import argparse

class BatchToStreamTransformer(ast.NodeTransformer):
    def __init__(self):
        super().__init__()
        self.df_name = None

    def visit_Assign(self, node):
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute):
            if node.value.func.attr == "csv":
                self.df_name = node.targets[0].id

                # Use spark.readStream instead of spark.read
                read_stream_base = ast.Attribute(
                    value=ast.Name(id='spark', ctx=ast.Load()),
                    attr='readStream',
                    ctx=ast.Load()
                )

                read_node = ast.Call(
                    func=ast.Attribute(value=read_stream_base, attr='option', ctx=ast.Load()),
                    args=[ast.Str(s='header'), ast.NameConstant(value=True)],
                    keywords=[]
                )
                read_node = ast.Call(
                    func=ast.Attribute(value=read_node, attr='option', ctx=ast.Load()),
                    args=[ast.Str(s='maxFilesPerTrigger'), ast.Str(s='1')],
                    keywords=[]
                )
                read_node = ast.Call(
                    func=ast.Attribute(value=read_node, attr='schema', ctx=ast.Load()),
                    args=[ast.Name(id='schema', ctx=ast.Load())],
                    keywords=[]
                )
                read_node = ast.Call(
                    func=ast.Attribute(value=read_node, attr='csv', ctx=ast.Load()),
                    args=[ast.Str(s='stream_input/')],
                    keywords=[]
                )
                node.value = read_node
        return node

    def visit_Expr(self, node):
        # Detect and replace df.write.csv(...) as an expression
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute):
            if node.value.func.attr == 'csv':
                # Correct base: df_grouped.writeStream (not callable!)
                write_stream_base = ast.Attribute(
                    value=ast.Name(id='df_grouped', ctx=ast.Load()),
                    attr='writeStream',
                    ctx=ast.Load()
                )

                query_node = ast.Call(
                    func=ast.Attribute(value=write_stream_base, attr='outputMode', ctx=ast.Load()),
                    args=[ast.Str(s='complete')], keywords=[]
                )
                query_node = ast.Call(
                    func=ast.Attribute(value=query_node, attr='format', ctx=ast.Load()),
                    args=[ast.Str(s='console')], keywords=[]
                )
                query_node = ast.Call(
                    func=ast.Attribute(value=query_node, attr='start', ctx=ast.Load()),
                    args=[], keywords=[]
                )

                return ast.Assign(
                    targets=[ast.Name(id='query', ctx=ast.Store())],
                    value=query_node
                )
        return node

# ===== MAIN EXECUTION =====

parser = argparse.ArgumentParser(description="Convert batch.py to stream.py using AST")
parser.add_argument("input_file", help="Path to batch.py")
args = parser.parse_args()

with open(args.input_file, "r") as f:
    batch_code = f.read()

tree = ast.parse(batch_code)
transformer = BatchToStreamTransformer()
transformed_tree = transformer.visit(tree)
stream_logic = astor.to_source(transformed_tree)

# Header for streaming
stream_header = '''from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("StreamingProcessingExample").getOrCreate()

schema = StructType([
    StructField("id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("value", StringType(), True)
])
'''

# Footer
stream_footer = '\nquery.awaitTermination()\n'

# Final assembly..
final_code = stream_header + "\n" + stream_logic + stream_footer

with open("stream.py", "w") as f:
    f.write(final_code)

print("✅ Final working stream.py generated successfully!")
