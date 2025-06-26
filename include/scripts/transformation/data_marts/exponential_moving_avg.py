from include.aws.glue_job_init import init_glue_job

from pyspark.sql.functions import *
from pyspark.sql.types import *

args, spark, glueContext = init_glue_job(["output_table", "ds", "input_table", "last_run_date"])

output_table = args['output_table']
run_date = args['ds']
input_table = args['input_table']
last_run_date = args['last_run_date']

create_table_query = f"""
    create table if not exists {output_table} (
        ticker STRING,
        cumulative_51_day_close_price ARRAY<DOUBLE>,
        12_day_ema double,
        26_day_ema double,
        50_day_ema double,
        date DATE
    )
    using iceberg
    partitioned by (date)
"""
spark.sql(create_table_query)

ema_cumulative_query = f"""
with today_data as (
    select * from {input_table}
    where date = '{run_date.date()}'
),
yesterday_data as (
    select * from {output_table}
    where date = '{last_run_date.date()}'
),
cumulative_51_day_close_price as (
    select
        coalesce(t.ticker, y.ticker) as ticker,
        y.12_day_ema as yesterday_12_day_ema,
        y.26_day_ema as yesterday_26_day_ema,
        y.50_day_ema as yesterday_50_day_ema,
        slice(concat(array(t.aggregates['close']), coalesce(y.cumulative_51_day_close_price, array())), 1, 51) as cumulative_51_day_close_price,
            CAST('{run_date.date()}' as DATE) as date
    from today_data t
    full outer join yesterday_data y on t.ticker = y.ticker
),
cumulative_ema as (
    select
        ticker,
        cumulative_51_day_close_price,
        case
            when size(cumulative_51_day_close_price) = 12
                then aggregate(slice(cumulative_51_day_close_price, 1, 12), cast(0.0 as double), (acc, x) -> acc + x) / 12
            when size(cumulative_51_day_close_price) > 12
                then cumulative_51_day_close_price[0] * (2.0 / 13) +  yesterday_12_day_ema * (11.0 / 13)
            else null
        end as 12_day_ema,
        case
            when size(cumulative_51_day_close_price) = 26
                then aggregate(slice(cumulative_51_day_close_price, 1, 26), cast(0.0 as double), (acc, x) -> acc + x) / 26
            when size(cumulative_51_day_close_price) > 26
                then cumulative_51_day_close_price[0] * (2.0 / 27) +  yesterday_26_day_ema * (25.0 / 27)
            else null
        end as 26_day_ema,
        case
            when size(cumulative_51_day_close_price) = 50
                then aggregate(slice(cumulative_51_day_close_price, 1, 50), cast(0.0 as double), (acc, x) -> acc + x) / 50
            when size(cumulative_51_day_close_price) > 50
                then cumulative_51_day_close_price[0] * (2.0 / 51) +  yesterday_50_day_ema * (49.0 / 51)
            else null
        end as 50_day_ema,
        date
    from cumulative_51_day_close_price
),
final_data as (
    select
        ticker,
        cumulative_51_day_close_price,
        12_day_ema,
        26_day_ema,
        50_day_ema,
        date
    from cumulative_ema
)
select * from final_data
"""

spark.sql(ema_cumulative_query).writeTo(output_table).overwritePartitions()