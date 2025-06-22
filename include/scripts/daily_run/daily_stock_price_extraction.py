import sys
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
import requests
import time

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# class DailyStockPriceExtraction:
#     """
#         This class is used to extract the daily stock price data from the Polygon API.
#     """

#     def __init__(self, spark_session: SparkSession, glue_context: GlueContext):
#         self.spark = spark_session
#         self.glue_context = glue_context
#         self.logger = self._setup_logging()

#     def __setup_logging(self) -> logging.Logger:
#         """Set up logging configuration"""
#         logger = logging.getLogger(__name__)
#         logger.setLevel(logging.INFO)

#         if not logger.handlers:
#             handler = logging.StreamHandler()
#             formatter = logging.Formatter(
#                 '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#             )
#             handler.setFormatter(formatter)
#             logger.addHandler(handler)
            
#         return logger


spark = (SparkSession.builder.getOrCreate())
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ["polygon_credentials", "output_table", "JOB_NAME", "ds"])
polygon_credentials = json.loads(args['polygon_credentials'])
output_table = args['output_table']
run_date = datetime.strptime(args['ds'], "%Y-%m-%d")

BASE_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/" + run_date.strftime("%Y-%m-%d")

params = {
    'apiKey' : polygon_credentials['AWS_SECRET_ACCESS_KEY'],
    'adjusted' : 'true'
}

url = BASE_URL

all_stock_aggregate = []

while url:
    response = requests.get(url, params=params)
    data = response.json()
    status = data.get('status')
    if status == "OK":
        results = data.get('results')
        if results:
            for stock in results:
                stock['o'] = float(stock.get('o')) if stock.get('o') is not None else None
                stock['h'] = float(stock.get('h')) if stock.get('h') is not None else None
                stock['l'] = float(stock.get('l')) if stock.get('l') is not None else None
                stock['c'] = float(stock.get('c')) if stock.get('c') is not None else None
                stock['v'] = float(stock.get('v')) if stock.get('v') is not None else None
                stock['date'] = stock['t'] if stock.get('t') is not None else None
                all_stock_aggregate.append(stock)

    else:
        print(f"Aggregated price not available for {run_date}. Maybe the market was closed")

    url = data.get("next_url")
    if url:
        time.sleep(1)  

schema = StructType([
    StructField("T", StringType(), False), # ticker
    StructField("c", DoubleType(), False), # close
    StructField("h", DoubleType(), False), # high
    StructField("l", DoubleType(), False), # low
    StructField("n", IntegerType(), True), # number of transactions
    StructField("o", DoubleType(), False), # open
    StructField("date", LongType(), False), # timestamp
    StructField("v", DoubleType(), False) # volume
])

print("one of the stock aggregate", all_stock_aggregate[0])

df = spark.createDataFrame(all_stock_aggregate, schema)

df = df \
        .withColumn("date", from_unixtime(col("date")/lit(1000)).cast('date')) \
        .withColumnRenamed("T", "ticker") \
        .withColumnRenamed("v", "volume") \
        .withColumnRenamed("n", "transactions") \
        .withColumnRenamed("o", "open") \
        .withColumnRenamed("c", "close") \
        .withColumnRenamed("h", "high") \
        .withColumnRenamed("l", "low")

df.writeTo(output_table).overwritePartitions()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)