import sys
from awsglue.utils import getResolvedOptions # this is to retrieve the parameters that are passed as part of the glue job
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

spark = (SparkSession.builder.getOrCreate())
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ["polygon_credentials", "output_table", "JOB_NAME"])
polygon_credentials = json.loads(args['polygon_credentials'])
output_table = args['output_table']


# this is to fetch historical data

s3_bucket = "s3a://flatfiles"

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", polygon_credentials['AWS_ACCESS_KEY_ID'])
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", polygon_credentials['AWS_SECRET_ACCESS_KEY'])
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://files.polygon.io/")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

create_table_query = f"""
    CREATE OR REPLACE TABLE {output_table} (
        ticker STRING,
        volume DOUBLE,
        open DOUBLE,  
        close DOUBLE,
        high DOUBLE,
        low DOUBLE,
        date DATE,
        transactions INTEGER 
    )
    USING iceberg
    PARTITIONED BY (date)
"""

# creating table if it does not exists
spark.sql(create_table_query)

years = [2021, 2022, 2023, 2024, 2025]

for year in years:
    file_path = f"{s3_bucket}/us_stocks_sip/day_aggs_v1/{year}/"

    df = spark.read \
            .format("csv") \
                .option("header", "true") \
                    .option("inferSchema", "true") \
                        .option("recursiveFileLookup", "true") \
                            .load(file_path)
    print(f"reading completed for year {year}")
    
    df2 = df.withColumn("date", from_unixtime(col("window_start")/lit(1000*1000*1000)).cast('date')) \
        .select('ticker', 'volume', 'open', 'close', 'high', 'low', 'date', 'transactions')
    df2.writeTo(output_table).using("iceberg").partitionedBy("date").overwritePartitions()
    
print("done ingesting the iceberg table")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)