from include.aws.glue_job_init import init_glue_job
from include.utils.make_api_request import make_api_request

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

args, spark, glueContext = init_glue_job(["polygon_credentials", "output_table", "ds", "JOB_NAME", "base_url"])

polygon_credentials = args['polygon_credentials']
output_table = args['output_table']
run_date = args['ds']
base_url = args['base_url']

params = {
    'limit' : 1000,
    'apiKey' : polygon_credentials['AWS_SECRET_ACCESS_KEY']
}

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        ticker STRING,
        name STRING,
        market STRING,
        locale STRING,
        primary_exchange STRING,
        type STRING,
        active BOOLEAN,
        extraction_date DATE
    )
    USING iceberg
"""
spark.sql(create_table_query)

all_tickers = []
url = base_url

cols_to_be_fetched = ["ticker", "name", "market", "locale", "primary_exchange", "type", "active"]

while url:
    success, data, error = make_api_request(url, params=params)

    if not success:
        print(f"Error fetching news for {run_date}: {error}")
        if "Client error" in error:
            break
        else:
            url = None
            break
     
    status = data.get('status')
    resCount = data.get('count')
    if status == "OK" and resCount > 0:
        results = data.get('results')
        if results:
            for item in results:
                ticker_result = {
                    col : item.get(col, None) for col in cols_to_be_fetched
                }
                ticker_result['extraction_date'] = run_date;
                all_tickers.append(ticker_result)
    else:
        print(f"API returned non-OK status: {status}")
        err_msg = data.get('error', "Unknown error")
        print(f"Error messsage: {err_msg}")

    url = data.get("next_url") # get the next URL
    if url:
        time.sleep(1) # not hitting the rate limit

if len(all_tickers) > 0:
    schema = StructType([
        StructField("ticker", StringType(), False),
        StructField("market", StringType(), False),
        StructField("locale", StringType(), False),
        StructField("active", BooleanType(), False),
        StructField("extraction_date", DateType(), False),

        StructField("primary_exchange", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True)
    ])

    df = spark.createDataFrame(all_tickers, schema)

    df.printSchema()

    df.writeTo(output_table).overwritePartitions()


