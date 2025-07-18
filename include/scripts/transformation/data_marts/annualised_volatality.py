from include.aws.glue_job_init import init_glue_job

from pyspark.sql.functions import *
from pyspark.sql.types import *

from include.utils.helper_functions import mean_last_n, sum_squared_deviations

from datetime import timedelta

args, spark, glueContext = init_glue_job(["output_table", "ds", "input_table", "last_run_date", "JOB_NAME"])

output_table = args['output_table']
run_date = args['ds']
input_table = args['input_table']
last_run_date = args['last_run_date']

print(f"last_run_date: {last_run_date}")

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
        select ticker, aggregates['close'] as close_price from {input_table}
        where date = '{run_date}'
    ),
    yesterday_cp_data as (
        select ticker, aggregates['close'] as close_price from {input_table}
        where date = '{last_run_date}'
    ),
    yesterday_daily_returns as (
        select ticker, cumulative_30_day_returns from {output_table}
        where date = '{last_run_date}'
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
    .withColumn("annualised_7d_volatility", sqrt("variance_7d")*sqrt(lit(252))) # multiply by sqrt(252) to get annualised volatility
    .withColumn("annualised_30d_volatility", sqrt("variance_30d")*sqrt(lit(252))) # multiply by sqrt(252) to get annualised volatility
    .drop("n", "sum_squared_deviations_7d", "sum_squared_deviations_30d", "variance_7d", "variance_30d", "mean_7d", "mean_30d")    # keep the DF tidy
    .withColumn("date", to_date(lit(run_date)))
)

df_with_std = df_with_std.select("ticker", "cumulative_30_day_returns", "annualised_7d_volatility", "annualised_30d_volatility", "date")

df_with_std.writeTo(output_table).overwritePartitions()