from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Column

import sys
import json
from datetime import datetime

def mean_last_n (col_name: str, count_col : str, n: int) -> Column:
    return (
        when(col(count_col) >= n,
             expr(
                 f"aggregate(slice({col_name}, 1, {n}), cast(0.0 as double), (acc, x) -> acc + x) / {n}"
                 )
                 ).otherwise(lit(None)))

def sum_squared_deviations (col_name: str, count_col : str, mean_col : str, n: int) -> Column:
    return (
        when(col(count_col) >= n,
             expr(
                 f"aggregate(slice({col_name}, 1, {n}), cast(0.0 as double), (acc, x) -> acc + pow(x - {mean_col}, 2))"
             )
             ).otherwise(lit(None)))

spark = (SparkSession.builder.getOrCreate())
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session


args = getResolvedOptions(sys.argv, ["output_table", "JOB_NAME", "ds", "fct_table", "last_run_date"])
fct_table = args["fct_table"]
output_table = args["output_table"]
run_date = args["ds"]
run_date = datetime.strptime(run_date, "%Y-%m-%d")
last_run_date = args["last_run_date"]
last_run_date = datetime.strptime(last_run_date, "%Y-%m-%d")

create_table_query = f"""
    create table if not exists {output_table} (
        ticker STRING,
        cumulative_30_day_returns ARRAY<DOUBLE>,
        annualised_7d_volatility DOUBLE,
        annualised_30d_volatility DOUBLE,
        date DATE
    )
    USING iceberg
    PARTITIONED BY (date)
"""

spark.sql(create_table_query)

annualised_volatility_query = f"""
    with today_cp_data as (
        select ticker, aggregates['close'] as close_price from {fct_table}
        where date = '{run_date.date()}'
    ),
    yesterday_cp_data as (
        select ticker, aggregates['close'] as close_price from {fct_table}
        where date = '{last_run_date.date()}'
    ),
    yesterday_daily_returns as (
        select ticker, cumulative_30_day_returns from {output_table}
        where date = '{last_run_date.date()}'
    ),
    daily_returns as (
        select
            coalesce(t.ticker, y.ticker) as ticker,
            case when exists(array(t.close_price, y.close_price), x -> x is null) then null else (t.close_price / y.close_price - 1)*100 end as daily_return
            from today_cp_data t
            full outer join yesterday_cp_data y on t.ticker = y.ticker
    ),
    cumulative_30_day_returns as (
        select
            d.ticker as ticker,
            case when daily_return is null then null 
            else slice(concat(array(daily_return), coalesce(y.cumulative_30_day_returns, array())), 1, 30) 
            end as cumulative_30_day_returns
        from daily_returns d
        full outer join yesterday_daily_returns y on d.ticker = y.ticker
    )
    select * from cumulative_30_day_returns
"""

df = spark.sql(annualised_volatility_query)

df_with_std = (
    df
    # n = size of the array
    .withColumn("n", size("cumulative_30_day_returns"))

    # Σx   : sum of returns
    .withColumn("mean_7d", mean_last_n("cumulative_30_day_returns", "n", 7))
    .withColumn("mean_30d", mean_last_n("cumulative_30_day_returns", "n", 30))

    # Σx²  : sum of squared returns
    .withColumn(
        "sum_squared_deviations_7d",
        sum_squared_deviations("cumulative_30_day_returns", "n", "mean_7d", 7)
    )
    .withColumn(
        "sum_squared_deviations_30d",
        sum_squared_deviations("cumulative_30_day_returns", "n", "mean_30d", 30)
    )

    # variance = (Σx² / n) – (mean)²
    .withColumn(
        "variance_7d",
        col("sum_squared_deviations_7d") / col("n")
    )
    .withColumn(
        "variance_30d",
        col("sum_squared_deviations_30d") / col("n")
    )
    .withColumn("annualised_7d_volatility", sqrt("variance_7d"))
    .withColumn("annualised_30d_volatility", sqrt("variance_30d"))
    .drop("n", "sum_squared_deviations_7d", "sum_squared_deviations_30d", "variance_7d", "variance_30d", "mean_7d", "mean_30d")    # keep the DF tidy
    .withColumn("date", lit(run_date.date()))
)

df_with_std = df_with_std.select("ticker", "cumulative_30_day_returns", "annualised_7d_volatility", "annualised_30d_volatility", "date")

df_with_std.writeTo(output_table).overwritePartitions()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)   