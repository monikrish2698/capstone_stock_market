from databricks.connect import DatabricksSession

try:
    spark = DatabricksSession.builder.remote(
        host = f"",
        token = "",
        cluster_id = ""
    ).getOrCreate()
except:
    raise Exception("connection failed")

df = spark.read.table("")

df.show(5)