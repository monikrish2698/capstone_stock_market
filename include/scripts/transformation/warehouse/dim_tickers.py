from include.aws.glue_job_init import init_glue_job

from pyspark.sql.functions import *

args, spark, glueContext = init_glue_job(["JOB_NAME", "output_table", "ds", "input_table", "overview", "sic_description", "ticker_types"])

output_table = args["output_table"]
run_date = args["ds"]
input_table = args["input_table"]
overview_table = args["overview"]
sic_description_table = args["sic_description"]
ticker_types_table = args["ticker_types"]

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        ticker STRING,
        name STRING,
        description STRING,
        market STRING,
        locale STRING,
        primary_exchange STRING,
        type STRING,
        type_name STRING,
        sic_code INTEGER,
        sic_description STRING,
        market_cap DOUBLE,
        total_employees INTEGER,
        active boolean,
        date DATE
    )
    USING iceberg
    PARTITIONED BY (date)
"""

spark.sql(create_table_query)

all_tickers_df = spark.read.table(f"{input_table}")

all_tickers_df = all_tickers_df.select(
    col("ticker"),
    col("name"),
    col("market"),
    col("locale"),
    col("primary_exchange"),
    col("type"),
    col("active"),
)

overview_df = spark.read.table(f"{overview_table}")

overview_df = overview_df.select(
    col("ticker"),
    col("description"),
    col("sic_code"),
    col("market_cap"),
    col("total_employees")
)

overview_df = overview_df.withColumn("sic_code", col("sic_code").cast("integer"))

sic_description_df = spark.read.table(f"{sic_description_table}")

sic_description_df = sic_description_df.select(
    col("sic_code"),
    col("sic_description")
)

ticker_types_df = spark.read.table(f"{ticker_types_table}")

ticker_types_df = ticker_types_df.select(
    col("code"),
    col("description")
)


ticker_types_df = ticker_types_df.withColumnRenamed("description", "type_name").withColumnRenamed("code", "type")


all_tickers_df = all_tickers_df.join(overview_df, on="ticker", how="left")
all_tickers_df = all_tickers_df.join(sic_description_df, on="sic_code", how="left")
all_tickers_df = all_tickers_df.join(ticker_types_df, on="type", how="left")
all_tickers_df = all_tickers_df.withColumn("date", to_date(lit(run_date)))

all_tickers_df = all_tickers_df.select(
    col("ticker"),
    col("name"),
    col("description"),
    col("market"),
    col("locale"),
    col("primary_exchange"),
    col("type"),
    col("type_name"),
    col("sic_code"),
    col("sic_description"),
    col("market_cap"),
    col("total_employees"),
    col("active"),
    col("date")
)

all_tickers_df.writeTo(output_table).overwritePartitions()












