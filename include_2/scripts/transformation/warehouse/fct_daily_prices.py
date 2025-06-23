from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import sys
import json
from datetime import datetime

spark = (SparkSession.builder.getOrCreate())
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ["polygon_credentials", "output_table", "JOB_NAME", "ds", "branch", "stg_table"])
polygon_credentials = json.loads(args["polygon_credentials"])
stg_table = args["stg_table"]
output_table = args["output_table"]
run_date = args["ds"]
run_date = datetime.strptime(run_date, "%Y-%m-%d")
branch_name = args["branch"]

# create_table_query = f"""
#     CREATE TABLE IF NOT EXISTS {output_table}
#     USING iceberg
#     PARTITIONED BY (date) AS
#     SELECT
#         CAST(null as STRING) as ticker,
#         CAST(null as MAP<STRING, DOUBLE>) as aggregates,
#         CAST('{run_date.date()}' as DATE) as date
# """

create_table_query = f"""
    create table if not exists {output_table} (
        ticker STRING,
        cumulative_200_day_close_price ARRAY<DOUBLE>,
        cumulative_5_day_ma double,
        cumulative_20_day_ma double,
        cumulative_50_day_ma double,
        cumulative_100_day_ma double,
        cumulative_200_day_ma double,
        date DATE
    )
"""

spark.sql(create_table_query)

# stg_df = spark.read.option("branch", f"{branch_name}").table(f"{stg_table}")

# stg_df.writeTo(output_table).overwritePartitions()

# delete_branch_query = f"""
#     alter table {stg_table} drop branch {branch_name}
# """

# spark.sql(delete_branch_query)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)