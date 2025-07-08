from include.aws.glue_job_init import init_glue_job

from pyspark.sql.functions import *
from pyspark.sql.types import *

args, spark, glueContext = init_glue_job(["output_table", "JOB_NAME", "ds", "input_table", "last_run_date"])

output_table = args['output_table']
run_date = args['ds']
input_table = args['input_table']
last_run_date = args['last_run_date']



create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        ticker STRING,
        cumulative_9_day_macd ARRAY<DOUBLE>,
        macd_line DOUBLE,
        signal_line DOUBLE,
        signal_line_crossover STRING,
        zero_crossover STRING,
        date DATE
    )
    USING iceberg
    PARTITIONED BY (date)
"""

spark.sql(create_table_query)

check_value_exists_query = f"""
    select *, ema_12_day - ema_26_day as macd_value from {input_table}
    where date = '{run_date}'
"""

check_value_exists = spark.sql(check_value_exists_query)

check_val_df = check_value_exists.filter(col("macd_value").isNotNull())

if check_val_df.count() > 0:
    print("MACD value exists for today")

    macd_crossover_query = f"""
        with today_data as (
            select *, ema_12_day - ema_26_day as macd_value from {input_table}
            where date = '{run_date}'
        ),
        yesterday_data as (
            select * from {output_table}
            where date = '{last_run_date}'
        ),
        cumulative_9_day_macd_cte as (
            select
                coalesce(t.ticker, y.ticker) as ticker,
                case when t.macd_value is null then null 
                else slice(concat(array(t.macd_value), coalesce(y.cumulative_9_day_macd, array())), 1, 9) 
                end as cumulative_9_day_macd,
                t.macd_value as macd,
                y.macd_line as yesterday_macd_line,
                y.signal_line as yesterday_signal_line
            from
                today_data t
            full outer join yesterday_data y on t.ticker = y.ticker
        ),
        building_signal_line as (
            select 
                ticker,
                cumulative_9_day_macd,
                macd,
                yesterday_macd_line,
                yesterday_signal_line,
                case
                    when size(cumulative_9_day_macd) = 9
                        then aggregate(slice(cumulative_9_day_macd, 1, 9), cast(0.0 as double), (acc, x) -> acc + x) / 9
                    when size(cumulative_9_day_macd) > 9
                        then cumulative_9_day_macd[0] * (2.0 / 10) +  yesterday_signal_line * (8.0 / 10)
                    else null
                end as signal_line
            from
                cumulative_9_day_macd_cte
        ),
        flag_crossovers as (
            select
                ticker,
                cumulative_9_day_macd,
                macd as macd_line,
                signal_line,
                case
                    when macd >= signal_line and yesterday_macd_line < yesterday_signal_line then 'bullish_signal_cross'
                    when macd <= signal_line and yesterday_macd_line > yesterday_signal_line then 'bearish_signal_cross'
                else null
                end as signal_line_crossover,
                case 
                    when macd >= 0 and yesterday_macd_line < 0 then 'bullish_zero_cross'
                    when macd <= 0 and yesterday_macd_line > 0 then 'bearish_zero_cross'
                else null
                end as zero_crossover,
                CAST('{run_date}' as DATE) as date
            from
                building_signal_line
        )
        select * from flag_crossovers
    """

    df = spark.sql(macd_crossover_query)

    df = df.filter(col("macd_line").isNotNull())

    df.writeTo(output_table).overwritePartitions()

else:
    print("MACD value does not exist for today")

