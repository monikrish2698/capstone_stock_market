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

args = getResolvedOptions(sys.argv, ["output_table", "JOB_NAME", "ds", "data_lake", "fct_table"])
data_lake = args["data_lake"]
output_table = args["output_table"]
run_date = args["ds"]
run_date = datetime.strptime(run_date, "%Y-%m-%d")

create_table_query = f"""
    create table if not exists {output_table} (
        ticker STRING,
        related_tickers ARRAY<STRING>,
        date DATE
    )
    USING iceberg
    PARTITIONED BY (date)
"""
spark.sql(create_table_query)

related_tickers_query = f"""
    select ticker, related_tickers, CAST('{run_date.date()}' as DATE) as date
    from {data_lake}.related_tickers
"""

spark.sql(related_tickers_query).writeTo(output_table).overwritePartitions()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)