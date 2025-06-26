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

args = getResolvedOptions(sys.argv, ["polygon_credentials", "output_table", "JOB_NAME", "ds"])
polygon_credentials = json.loads(args["polygon_credentials"])
output_table = args["output_table"]
run_date = args["ds"]
run_date = datetime.strptime(run_date, "%Y-%m-%d")

BASE_URL = "https://api.polygon.io/v3/reference/tickers/types"

create_table_query = f"""
    CREATE OR REPLACE TABLE {output_table} (
        code STRING,
        description STRING,
        locale STRING,
        asset_class STRING,
        extraction_date DATE
    )
    USING iceberg
"""
spark.sql(create_table_query)

all_ticker_types = []
url = BASE_URL

params = {
    'limit' : 1000,
    'apiKey' : polygon_credentials['AWS_SECRET_ACCESS_KEY']
}

while url:
    response = requests.get(url, params=params)
    data = response.json()
    status = data.get('status')
    if status == "OK":
        results = data.get('results')
        if results:
            for item in results:
                item['extraction_date'] = run_date;
                all_ticker_types.append(item)

    url = data.get("next_url") # get the next URL

    if url:
        time.sleep(1) # not hitting the rate limit

schema = StructType([
    StructField("code", StringType(), False),
    StructField("description", StringType(), True),
    StructField("locale", StringType(), True),
    StructField("asset_class", StringType(), True),
    StructField("extraction_date", DateType(), False)
])

df = spark.createDataFrame(all_ticker_types, schema)
(
    df.writeTo(output_table)
        .overwritePartitions()
)

df.printSchema()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)







