from include.aws.glue_job_init import init_glue_job
from include.utils.make_api_request import make_api_request

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

args, spark, glueContext = init_glue_job(["polygon_credentials", "output_table", "ds", "JOB_NAME", "base_url", "input_table"])

polygon_credentials = args['polygon_credentials']
output_table = args['output_table']
run_date = args['ds']
base_url = args['base_url']
input_table = args['input_table']

params = {
    'apiKey' : polygon_credentials['AWS_SECRET_ACCESS_KEY']
}

create_table_query = f"""
    CREATE OR REPLACE TABLE {output_table} (
        ticker STRING,
        related_tickers ARRAY<STRING>,
        extraction_date DATE
    )
    USING iceberg
"""
spark.sql(create_table_query)

all_tickers = spark.table(f'{input_table}').createOrReplaceTempView('all_tickers')
all_related_tickers = []

distinct_tickers_list = [row['ticker'] for row in spark.sql("""
    select distinct ticker from all_tickers where market = 'stocks' and primary_exchange = 'XNAS'
""").collect()]

for ticker in distinct_tickers_list:
    
    url = f"{base_url}/{ticker}"
    success, data, error = make_api_request(url, params=params)

    if not success:
        print(f"Error fetching news for {run_date}: {error}")
        if "Client error" in error:
            break
        else:
            url = None
            break
    status = data.get('status')
    if status == "OK":
        results = data.get('results')
        if results:
            related_tickers = [ticker['ticker'] for ticker in results]
            all_related_tickers.append({
                'ticker' : ticker,
                'related_tickers' : related_tickers,
                'extraction_date' : run_date
            })
    else:
        print(f"API returned non-OK status: {status}")
        err_msg = data.get('error', "Unknown error")
        print(f"Error messsage: {err_msg}")
    time.sleep(1)

if len(all_related_tickers) > 0:
    schema = StructType([
        StructField("ticker", StringType(), False),
        StructField("related_tickers", ArrayType(StringType()), False),
        StructField("extraction_date", DateType(), False)
    ])
    df = spark.createDataFrame(all_related_tickers, schema)
    df.printSchema()
    df.writeTo(output_table).overwritePartitions()
