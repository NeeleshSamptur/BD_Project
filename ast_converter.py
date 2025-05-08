import ast
import astor
import argparse

class BatchToStreamTransformer(ast.NodeTransformer):
    def __init__(self):
        super().__init__()
        self.write_df_name = None
        self.has_write_stream = False

    def visit_Assign(self, node):
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute):
            if node.value.func.attr == "csv":
                print("🔹 Rewriting spark.read.csv → spark.readStream.csv")
                self.write_df_name = node.targets[0].id
                read_stream = ast.Call(
                    func=ast.Attribute(
                        value=ast.Attribute(
                            value=ast.Name(id='spark', ctx=ast.Load()),
                            attr='readStream',
                            ctx=ast.Load()
                        ),
                        attr='option',
                        ctx=ast.Load()
                    ),
                    args=[ast.Constant(value='header'), ast.Constant(value=True)],
                    keywords=[]
                )
                read_stream = ast.Call(
                    func=ast.Attribute(value=read_stream, attr='option', ctx=ast.Load()),
                    args=[ast.Constant(value='maxFilesPerTrigger'), ast.Constant(value='1')],
                    keywords=[]
                )
                read_stream = ast.Call(
                    func=ast.Attribute(value=read_stream, attr='schema', ctx=ast.Load()),
                    args=[ast.Name(id='schema', ctx=ast.Load())],
                    keywords=[]
                )
                read_stream = ast.Call(
                    func=ast.Attribute(value=read_stream, attr='csv', ctx=ast.Load()),
                    args=[ast.Constant(value='stream_input/')],
                    keywords=[]
                )
                node.value = read_stream
        return self.generic_visit(node)

    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute):
            if node.value.func.attr == "show":
                print(" Removing unsupported .show() for streaming")
                return None
            if node.value.func.attr == 'csv':
                print("🔹 Rewriting df.write.csv → df.writeStream.start")
                self.has_write_stream = True
                # find correct DataFrame name used in write
                base_df = node.value.func.value
                if isinstance(base_df, ast.Attribute) and base_df.attr == "write":
                    base_df = base_df.value

                write_stream = ast.Call(
                    func=ast.Attribute(
                        value=ast.Attribute(
                            value=base_df,
                            attr='writeStream',
                            ctx=ast.Load()
                        ),
                        attr='outputMode',
                        ctx=ast.Load()
                    ),
                    args=[ast.Constant(value='complete')],
                    keywords=[]
                )
                write_stream = ast.Call(
                    func=ast.Attribute(value=write_stream, attr='format', ctx=ast.Load()),
                    args=[ast.Constant(value='console')],
                    keywords=[]
                )
                write_stream = ast.Call(
                    func=ast.Attribute(value=write_stream, attr='start', ctx=ast.Load()),
                    args=[],
                    keywords=[]
                )
                return ast.Assign(targets=[ast.Name(id='query', ctx=ast.Store())], value=write_stream)
        return self.generic_visit(node)

    def visit_Module(self, node):
        node.body = [stmt for stmt in node.body if stmt is not None]
        self.generic_visit(node)
        return node

# ===== MAIN EXECUTION =====

parser = argparse.ArgumentParser(description="Convert batch DataFrame Spark code to structured streaming Spark code.")
parser.add_argument("input_file", help="Path to batch DataFrame Python file")
args = parser.parse_args()

with open(args.input_file, "r") as f:
    batch_code = f.read()

tree = ast.parse(batch_code)
transformer = BatchToStreamTransformer()
transformed_tree = transformer.visit(tree)

stream_logic = astor.to_source(transformed_tree)

stream_header = '''from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("StreamingProcessingExample").getOrCreate()

schema = StructType([
    StructField("timestamp", StringType(), True),  
    StructField("category", StringType(), True),
    StructField("value", DoubleType(), True)
])
'''

stream_footer = '\nquery.awaitTermination()\n' if transformer.has_write_stream else ''

final_code = stream_header + "\n" + stream_logic + stream_footer

with open("stream_output3.py", "w", encoding="utf-8") as f:
    f.write(final_code)

print("✅ Structured streaming code saved to 'stream_output2.py'")
