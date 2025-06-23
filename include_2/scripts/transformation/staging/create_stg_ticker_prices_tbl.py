from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import sys
import json
from datetime import datetime

spark = (SparkSession.builder.getOrCreate())
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ["polygon_credentials", "output_table", "JOB_NAME", "ds"])
polygon_credentials = json.loads(args["polygon_credentials"])
output_table = args["output_table"]
run_date = args["ds"]
run_date = datetime.strptime(run_date, "%Y-%m-%d")

create_table_query = f"""
    CREATE OR REPLACE TABLE {output_table}
    USING iceberg
    PARTITIONED BY (date)
    TBLPROPERTIES ( 'write.wap.enabled' = 'true' ) AS
    SELECT
        CAST(null as STRING) as ticker,
        CAST(null as MAP<STRING, DOUBLE>) as aggregates,
        CAST('{run_date.date()}' as DATE) as date
"""

spark.sql(create_table_query) # creating the staging table if it does not exist

job = Job(glueContext)
job.init(args["JOB_NAME"], args)