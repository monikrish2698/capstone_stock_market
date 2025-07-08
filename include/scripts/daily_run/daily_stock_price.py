from include.aws.glue_job_init import init_glue_job
from include.utils.make_api_request import make_api_request

from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
import time

args, spark, glueContext = init_glue_job(["polygon_credentials", "output_table", "ds", "JOB_NAME", "base_url"])
polygon_credentials = args['polygon_credentials']
output_table = args['output_table']
run_date = args['ds']
base_url = args['base_url']

params = {
    'apiKey' : polygon_credentials['AWS_SECRET_ACCESS_KEY'], 
    'published_utc' : run_date.strftime("%Y-%m-%d")
}

url = base_url + run_date.strftime("%Y-%m-%d")

all_stock_aggregate = []

while url:
    success, data, error = make_api_request(url, params=params)

    if not success:
        print(f"Error fetching news for {run_date}: {error}")
        if "Client error" in error:
            break
        else:
            url = None
            break

    resCount = data.get('resultsCount')
    status = data.get('status')
    if status == "OK" and resCount > 0:
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
        print(f"API returned non-OK status: {status}")
        err_msg = data.get('error', "Unknown error")
        print(f"Error messsage: {err_msg}")

    url = data.get("next_url")
    if url:
        time.sleep(1) 

if len(all_stock_aggregate) > 0:

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