from awsglue.utils import getResolvedOptions # this is to retrieve the parameters that are passed as part of the glue job
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

args = getResolvedOptions(sys.argv, ["polygon_credentials", "output_table", "JOB_NAME", "ds"])
polygon_credentials = json.loads(args["polygon_credentials"])
output_table = args["output_table"]
run_date = args["ds"]
run_date = datetime.strptime(run_date, "%Y-%m-%d")

BASE_URL = "https://api.polygon.io/v3/reference/tickers"


create_table_query = f"""
    CREATE OR REPLACE TABLE {output_table} (
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
url = BASE_URL

params = {
    'limit' : 1000,
    'apiKey' : polygon_credentials['AWS_SECRET_ACCESS_KEY']
}

cols_to_be_fetched = ["ticker", "name", "market", "locale", "primary_exchange", "type", "active"]

while url:
    response = requests.get(url, params=params)
    data = response.json()
    status = data.get('status')
    if status == "OK":
        results = data.get('results')
        if results:
            for item in results:
                ticker_result = {
                    col : item.get(col, None) for col in cols_to_be_fetched
                }
                ticker_result['extraction_date'] = run_date;
                all_tickers.append(ticker_result)

    url = data.get("next_url") # get the next URL

    if url:
        time.sleep(1) # not hitting the rate limit

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

(
    df.writeTo(output_table)
        .overwritePartitions()
)

df.printSchema()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)