from include.utils.make_api_request import make_api_request
from include.aws.glue_job_init import init_glue_job

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

args, spark, glueContext = init_glue_job(["polygon_credentials", "output_table", "ds", "input_table", "base_url", "JOB_NAME"])
polygon_credentials = args['polygon_credentials']
output_table = args['output_table']
run_date = args['ds']
input_table = args['input_table']
base_url = args['base_url']

all_tickers = spark.table(f'{input_table}').createOrReplaceTempView('all_tickers')

distinct_tickers_list = [row['ticker'] for row in spark.sql("""
    select distinct ticker from all_tickers where market = 'stocks' and primary_exchange = 'XNAS'
""").collect()]

tickers_overview = []

create_table_query = f"""
    CREATE OR REPLACE TABLE {output_table} (
        ticker STRING,
        name STRING,
        description STRING,
        sic_code STRING,
        sic_description STRING,
        market_cap DOUBLE,  
        total_employees INT,
        extraction_date DATE
    )
    USING iceberg
"""

spark.sql(create_table_query)

params = {
    'apiKey' : polygon_credentials['AWS_SECRET_ACCESS_KEY']
    }

cols_to_be_fetched = [ 'ticker', 'name', 'description', 'sic_code', 'sic_description', 'market_cap', 'total_employees', 'extraction_date' ]

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
            ticker_overview = {
                col : results.get(col, None) for col in cols_to_be_fetched
            }
            ticker_overview['extraction_date'] = run_date;
            tickers_overview.append(ticker_overview)
    else:
        print(f"API returned non-OK status: {status}")
        err_msg = data.get('error', "Unknown error")
        print(f"Error messsage: {err_msg}")

    time.sleep(1)

if len(tickers_overview) > 0:
    schema = StructType([
        StructField("ticker", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("sic_code", StringType(), True),
        StructField("sic_description", StringType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("total_employees", IntegerType(), True),
        StructField("extraction_date", DateType(), False)
    ])

    df = spark.createDataFrame(tickers_overview, schema).withColumn('extraction_date', lit(run_date))

    df.printSchema()

    df.writeTo(output_table).overwritePartitions()

