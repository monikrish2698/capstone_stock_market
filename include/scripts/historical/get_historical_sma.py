from include.aws.glue_job_init import init_glue_job
from include.utils.make_api_request import make_api_request

from datetime import date
import time

from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

args, spark, glueContext = init_glue_job(["output_table", "date", "JOB_NAME", "input_table", "polygon_credentials"])

output_table = args['output_table']
run_date = args['date']
job_name = args['JOB_NAME']
input_table = args['input_table']
polygon_credentials = args['polygon_credentials']

schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("date",   DateType(),   True),
    StructField("sma",    DoubleType(), True)   # the column will be renamed later
])

def build_df(results: list, col_name: str):
    """
    Turn the Polygon result list into a Spark DF.
    Returns an **empty** DF with the same schema if results is falsy.
    """
    rows = [
        (ticker,
         date.fromtimestamp(r['timestamp'] // 1000),
         float(r['value']))
        for r in (results or [])
    ]
    df = spark.createDataFrame(rows, schema)
    return df.withColumnRenamed("sma", col_name)

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        ticker STRING,
        12_day_sma DOUBLE,
        26_day_sma DOUBLE,
        50_day_sma DOUBLE,
        date DATE
    )
    USING iceberg
"""

spark.sql(create_table_query)

sample_population_query = f"""
    with companies as (
        select * from {input_table}
        where market_cap >= 2000000000 and market_cap <= 200000000000
    )
    select distinct ticker from companies TABLESAMPLE (10 PERCENT)
"""

sample_population = spark.sql(sample_population_query)

tickers = [row.ticker for row in sample_population.select("ticker").collect()]

params = {
    'apiKey' : polygon_credentials['AWS_SECRET_ACCESS_KEY'], 
    'timestamp.gte' : run_date
}

for ticker in tickers:
    sma_12_day = f"https://api.polygon.io/v1/indicators/sma/{ticker}?timespan=day&adjusted=true&window=12&series_type=close&order=desc&limit=5000"
    sma_26_day = f"https://api.polygon.io/v1/indicators/sma/{ticker}?timespan=day&adjusted=true&window=26&series_type=close&order=desc&limit=5000"
    sma_50_day = f"https://api.polygon.io/v1/indicators/sma/{ticker}?timespan=day&adjusted=true&window=50&series_type=close&order=desc&limit=5000"

    success, data12, error = make_api_request(sma_12_day, params=params)
    success, data26, error = make_api_request(sma_26_day, params=params)
    success, data50, error = make_api_request(sma_50_day, params=params)

    results12 = data12.get('results').get('values')
    results26 = data26.get('results').get('values')
    results50 = data50.get('results').get('values')

    print("sample", results12[0])

    df12 = build_df(results12, "12_day_sma")
    df26 = build_df(results26, "26_day_sma")
    df50 = build_df(results50, "50_day_sma")

    tmp_df = df12.join(df26, on=["ticker", "date"], how="full") \
        .join(df50, on=["ticker", "date"], how="full")
    
    tmp_df.writeTo(output_table).append()

    time.sleep(1)

    print(f"Processed SMA for {ticker}")
    
    
    







