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

args = getResolvedOptions(sys.argv, ["polygon_credentials", "output_table", "JOB_NAME", "ds", "fct_table", "last_run_date"])
polygon_credentials = json.loads(args["polygon_credentials"])
fct_table = args["fct_table"]
output_table = args["output_table"]
run_date = args["ds"]
run_date = datetime.strptime(run_date, "%Y-%m-%d")
last_run_date = args["last_run_date"]
last_run_date = datetime.strptime(last_run_date, "%Y-%m-%d")


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

spark.sql(create_table_query) # creating table if it does not exist

cumulative_avg_query = f"""
with today_data as (
    select 
        *
    from
        {fct_table}
    where
        date = '{run_date.date()}'
),
yesterday_data as (
    select
        *
    from
        {output_table}
    where
        date = '{last_run_date.date()}'
),
cumulative_200_day_close_price as (
    select
        coalesce(t.ticker, y.ticker) as ticker,
        slice(
            concat(
                array(t.aggregates['close']), 
                    coalesce(y.cumulative_200_day_close_price, array())
                    ), 1, 200) as cumulative_200_day_close_price,
        {run_date.date()} as date
    from today_data t
    full outer join yesterday_data y on t.ticker = y.ticker
),
cumulative_ma as (
    select

)

"""