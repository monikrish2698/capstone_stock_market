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

BASE_URL = "https://api.polygon.io/v2/reference/news"

params = {
    'apiKey' : polygon_credentials['AWS_SECRET_ACCESS_KEY'], 
    'published_utc' : run_date.strftime("%Y-%m-%d")
}

cols_to_be_fetched = ["id", "title", "article_url", "tickers", "description", "keywords", "insights", "published_utc"]

all_news = []

create_table_query = f"""
    CREATE OR REPLACE TABLE {output_table} (
        id STRING,
        title STRING,
        article_url STRING,
        tickers ARRAY<STRING>,
        description STRING,
        keywords ARRAY<STRING>,
        insights ARRAY<STRUCT<
            ticker : STRING,
            sentiment : STRING,
            sentiment_reasoning : STRING
        >>,
        published_utc STRING,
        extraction_date DATE
    )
    USING iceberg
    PARTITIONED BY (extraction_date)
"""

spark.sql(create_table_query)

url = f"{BASE_URL}"

while url:
    response = requests.get(url, params=params)
    data = response.json()
    status = data.get('status')
    if status == "OK":
        results = data.get('results')
        if results:
            for article in results:
                article_result = {
                    col : article.get(col, None) for col in cols_to_be_fetched
                }
                all_news.append(article_result)
    else:
        print(f"News for {run_date} not found")
    time.sleep(1)

    url = data.get("next_url") # get the next URL

    if url:
        time.sleep(1)
    
schema = StructType([
    StructField("id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("article_url", StringType(), True),
    StructField("tickers", ArrayType(StringType()), True),
    StructField("description", StringType(), True),
    StructField("keywords", ArrayType(StringType()), True),
    StructField("insights", ArrayType(StructType([
        StructField("ticker", StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("sentiment_reasoning", StringType(), True)
    ])), True),
    StructField("published_utc", StringType(), False)
])

new_news = spark.createDataFrame(all_news, schema).withColumn('extraction_date', lit(run_date))

new_news.printSchema()

(
    new_news.writeTo(output_table)
        .overwritePartitions()
)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)