from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import sys
from datetime import datetime

spark = (SparkSession.builder.getOrCreate())
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ["output_table", "JOB_NAME", "ds", "schema"])
schema = args["schema"]
output_table = args["output_table"]
run_date = args["ds"]
run_date = datetime.strptime(run_date, "%Y-%m-%d")

create_table_query = f"""
    CREATE OR REPLACE TABLE {output_table} (
        id STRING,
        title STRING,
        article_url STRING,
        description STRING,
        keywords ARRAY<STRING>,
        tickers ARRAY<STRING>,
        ticker_level_insights MAP<STRING, STRUCT<sentiment: STRING, sentiment_reasoning: STRING>>,
        published_utc TIMESTAMP,
        date DATE
    )
    USING iceberg
    PARTITIONED BY (date)
"""

spark.sql(create_table_query)

df = spark.read.table(f"{schema}.daily_news").filter(col("extraction_date") == run_date)

df.printSchema()

transformed_df = df.withColumn("ticker_level_insights", 
                               map_from_entries(
                                   transform(
                                       "insights",
                                       lambda x: struct(
                                           x["ticker"],
                                           struct(
                                               x["sentiment"].alias("sentiment"),
                                               x["sentiment_reasoning"].alias("sentiment_reasoning")
                                           )
                                       )
                                   )
                               )) \
.withColumn("date", to_date(lit(run_date)))


transformed_df = transformed_df.select(
    col("id"), 
    col("title"),
    col("article_url"),
    col("description"),
    col("keywords"),
    col("tickers"),
    col("ticker_level_insights"),
    col("published_utc").cast("timestamp"),
    col("date")
)

transformed_df.writeTo(output_table).overwritePartitions()


job = Job(glueContext)
job.init(args["JOB_NAME"], args)