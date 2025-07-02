from include.aws.glue_job_init import init_glue_job

from pyspark.sql.functions import col

args, spark, glueContext = init_glue_job(["JOB_NAME", "ds", "branch", "input_table"])

input_table = args['input_table']
run_date = args['ds']
branch = args['branch']

query = f"""
    with basic_data_checks as (
        select count(1) > 0 as there_is_data, count(1) = count(distinct ticker) as deduplicated
        from {input_table} for version as of '{branch}'
        where date = date('{run_date}')
    ),
    negative_price_checks as (
        select count(1) = 0 as no_negative_prices
        from {input_table} for version as of '{branch}'
        where date = date('{run_date}') and (aggregates['close'] < 0 or aggregates['open'] < 0 or aggregates['low'] < 0 or aggregates['high'] < 0)
    ),
    null_price_checks as (
        select count(1) = 0 as no_null_prices
        from {input_table} for version as of '{branch}'
        where date = date('{run_date}') and (aggregates['close'] is null or aggregates['open'] is null or aggregates['low'] is null or aggregates['high'] is null)
    )
    select there_is_data, deduplicated, no_negative_prices, no_null_prices, (there_is_data and deduplicated and no_negative_prices and no_null_prices) as all_checks
    from basic_data_checks, negative_price_checks, null_price_checks
"""

basic_checks_df = spark.sql(query)

failed_checks = basic_checks_df.select(
    col("there_is_data"),
    col("deduplicated"),
    col("no_negative_prices"),
    col("no_null_prices"),
    col("all_checks")
).where(col("all_checks") == False).collect()

assert len(failed_checks) == 0, f"Data quality checks failed: {failed_checks}"
