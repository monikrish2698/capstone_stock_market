from include.aws.glue_job_init import init_glue_job

from pyspark.sql.functions import *
from pyspark.sql.types import *

args, spark, glueContext = init_glue_job(["output_table", "ds", "JOB_NAME", "input_table"])

output_table = args['output_table']
run_date = args['ds']
input_table = args['input_table']

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
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

df = spark.read.table(f"{input_table}").filter(col("extraction_date") == run_date)
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
                               )).withColumn("date", to_date(lit(run_date)))

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