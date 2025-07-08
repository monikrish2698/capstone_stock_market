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
url = base_url

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
                item['extraction_date'] = run_date;
                all_ticker_types.append(item)
    
    else:
        print(f"API returned non-OK status: {status}")
        err_msg = data.get('error', "Unknown error")
        print(f"Error messsage: {err_msg}")

    url = data.get("next_url") # get the next URL
    if url:
        time.sleep(1)

if len(all_ticker_types) > 0:
    schema = StructType([
        StructField("code", StringType(), False),
        StructField("description", StringType(), True),
        StructField("locale", StringType(), True),
        StructField("asset_class", StringType(), True),
        StructField("extraction_date", DateType(), False)
    ])

    df = spark.createDataFrame(all_ticker_types, schema)

    df.printSchema()

    df.writeTo(output_table).overwritePartitions()
