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

args = getResolvedOptions(sys.argv, ["polygon_credentials", "output_table", "JOB_NAME", "ds", "schema", "branch"])
polygon_credentials = json.loads(args["polygon_credentials"])
schema = args["schema"]
output_table = args["output_table"]
run_date = args["ds"]
run_date = datetime.strptime(run_date, "%Y-%m-%d")
branch_name = args["branch"] or "stg_daily_ticker"

transformed_df = spark.table(f"{schema}.daily_stock_prices") \
                    .filter(col("date") == run_date)


transformed_df.printSchema()

result_df = transformed_df.select(
            "ticker",
            create_map(
                lit("open"), round(col("open"), 2),
                lit("close"), round(col("close"), 2),
                lit("low"), round(col("low"), 2),
                lit("high"), round(col("high"), 2),
                lit("volume"), col("volume"),
                lit("transactions"), col("transactions").cast(DoubleType())
            ).alias("aggregates"),
            col("date"))

result_df.writeTo(output_table) \
    .option('branch', f"{branch_name}").overwritePartitions()

result_df.printSchema()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)



