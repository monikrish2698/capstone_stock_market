from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import sys
import json
import requests
import time
from datetime import datetime

spark = (SparkSession.builder.getOrCreate())
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ["polygon_credentials", "output_table", "JOB_NAME", "ds", "schema"])
polygon_credentials = json.loads(args["polygon_credentials"])
schema = args["schema"]
output_table = args["output_table"]
run_date = args["ds"]
run_date = datetime.strptime(run_date, "%Y-%m-%d")

BASE_URL = "https://api.polygon.io/v3/reference/tickers"

all_tickers = spark.table(f'{schema}.all_tickers').createOrReplaceTempView('all_tickers')
distinct_tickers_list = [row['ticker'] for row in spark.sql("""
    select distinct ticker from all_tickers where market = 'stocks' and primary_exchange = 'XNAS'
""").collect()]

tickers_overview = []

cols_to_be_fetched = [ 'ticker', 'name', 'description', 'sic_code', 'market_cap', 'total_employees', 'extraction_date' ]

for ticker in distinct_tickers_list:
    params = {
        'apiKey' : polygon_credentials['AWS_SECRET_ACCESS_KEY']
    }
    url = f"{BASE_URL}/{ticker}"
    response = requests.get(url, params=params)
    data = response.json()
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
        print(f"Overview for {ticker} not found")
    time.sleep(1)

schema = StructType([
    StructField("ticker", StringType(), False),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("sic_code", StringType(), True),
    StructField("market_cap", DoubleType(), True),
    StructField("total_employees", IntegerType(), True),
    StructField("extraction_date", DateType(), False)
])

df = spark.createDataFrame(tickers_overview, schema).withColumn('extraction_date', lit(run_date))

df.printSchema()

create_table_query = f"""
    CREATE OR REPLACE TABLE {output_table} (
        ticker STRING,
        name STRING,
        description STRING,
        sic_code STRING,
        market_cap DOUBLE,  
        total_employees INT,
        extraction_date DATE
    )
    USING iceberg
"""

spark.sql(create_table_query)

( df.writeTo(output_table).overwritePartitions() )

job = Job(glueContext)
job.init(args["JOB_NAME"], args)