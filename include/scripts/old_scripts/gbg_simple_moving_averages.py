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
    using iceberg
    partitioned by (date)
"""

spark.sql(create_table_query) # creating table if it does not exist

cumulative_avg_query = f"""
    with today_data as (
        select *
        from {fct_table}
        where date = '{run_date.date()}'
    ),
    yesterday_data as (
        select *
        from {output_table}
        where date = '{last_run_date.date()}'
    ),
    cumulative_200_day_close_price as (
        select
            coalesce(t.ticker, y.ticker) as ticker,
            slice(
                concat(
                    array(t.aggregates['close']), 
                        coalesce(y.cumulative_200_day_close_price, array())
                        ), 1, 200) as cumulative_200_day_close_price,
            CAST('{run_date.date()}' as DATE) as date
        from today_data t
        full outer join yesterday_data y on t.ticker = y.ticker
    ),
    cumulative_ma as (
        select
            ticker,
            cumulative_200_day_close_price,
            case
                when size(slice(cumulative_200_day_close_price, 1, 5)) = 5 
                    then aggregate(slice(cumulative_200_day_close_price, 1, 5), cast(0.0 as double), (acc, x) -> acc + x) / 5
                else null
            end as cumulative_5_day_ma,
            case
                when size(slice(cumulative_200_day_close_price, 1, 20)) = 20
                    then aggregate(slice(cumulative_200_day_close_price, 1, 20), cast(0.0 as double), (acc, x) -> acc + x) / 20
                else null
            end as cumulative_20_day_ma,
            case
                when size(slice(cumulative_200_day_close_price, 1, 50)) = 50
                    then aggregate(slice(cumulative_200_day_close_price, 1, 50), cast(0.0 as double), (acc, x) -> acc + x) / 50
                else null
            end as cumulative_50_day_ma,
            case
                when size(slice(cumulative_200_day_close_price, 1, 100)) = 100
                    then aggregate(slice(cumulative_200_day_close_price, 1, 100), cast(0.0 as double), (acc, x) -> acc + x) / 100
                else null
            end as cumulative_100_day_ma,
            case
                when size(slice(cumulative_200_day_close_price, 1, 200)) = 200
                    then aggregate(slice(cumulative_200_day_close_price, 1, 200), cast(0.0 as double), (acc, x) -> acc + x) / 200
                else null
            end as cumulative_200_day_ma,
            date
        from cumulative_200_day_close_price
    ),
    final_data as (
        select
            ticker,
            cumulative_200_day_close_price,
            cumulative_5_day_ma,
            cumulative_20_day_ma,
            cumulative_50_day_ma,
            cumulative_100_day_ma,
            cumulative_200_day_ma,
            date
        from cumulative_ma
    )
    select * from final_data
"""

spark.sql(cumulative_avg_query).writeTo(output_table).overwritePartitions()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)