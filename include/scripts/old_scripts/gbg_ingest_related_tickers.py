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

BASE_URL = "https://api.polygon.io/v1/related-companies"

all_tickers = spark.table(f'{schema}.all_tickers').createOrReplaceTempView('all_tickers')

all_related_tickers = []

# only fetch tickers from NASDAQ
distinct_tickers_list = [row['ticker'] for row in spark.sql("""
    select distinct ticker from all_tickers where market = 'stocks' and primary_exchange = 'XNAS'
""").collect()]

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
            related_tickers = [ticker['ticker'] for ticker in results]
            all_related_tickers.append({
                'ticker' : ticker,
                'related_tickers' : related_tickers,
                'extraction_date' : run_date
            })
    else:
        print(f"Related tickers for {related_tickers} not found")
    time.sleep(1)

schema = StructType([
    StructField("ticker", StringType(), False),
    StructField("related_tickers", ArrayType(StringType()), False),
    StructField("extraction_date", DateType(), False)
])

create_table_query = f"""
    CREATE OR REPLACE TABLE {output_table} (
        ticker STRING,
        related_tickers ARRAY<STRING>,
        extraction_date DATE
    )
    USING iceberg
"""

spark.sql(create_table_query)

df = spark.createDataFrame(all_related_tickers, schema)

df.printSchema()

( df.writeTo(output_table).overwritePartitions() )

job = Job(glueContext)
job.init(args["JOB_NAME"], args)




