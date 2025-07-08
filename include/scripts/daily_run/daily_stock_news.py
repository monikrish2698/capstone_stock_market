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
    'apiKey' : polygon_credentials['AWS_SECRET_ACCESS_KEY'], 
    'published_utc' : run_date.strftime("%Y-%m-%d")
}

cols_to_be_fetched = ["id", "title", "article_url", "tickers", "description", "keywords", "insights", "published_utc"]

all_news = []

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
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

url = f"{base_url}"

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
            for article in results:
                article_result = {
                    col : article.get(col, None) for col in cols_to_be_fetched
                }
                all_news.append(article_result)
    else:
        print(f"API returned non-OK status: {status}")
        err_msg = data.get('error', "Unknown error")
        print(f"Error messsage: {err_msg}")

    url = data.get("next_url") # get the next URL
    if url:
        time.sleep(1)

if len(all_news) > 0:    
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

    new_news.writeTo(output_table) \
        .overwritePartitions()