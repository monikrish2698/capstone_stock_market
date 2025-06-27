from include.aws.glue_job_init import init_glue_job

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

args, spark, glueContext = init_glue_job(["polygon_credentials", "output_table", "ds", "JOB_NAME", "input_table", "branch"])

polygon_credentials = args['polygon_credentials']
output_table = args['output_table']
run_date = args['ds']
branch = args['branch']
input_table = args['input_table']

transformed_df = spark.table(f"{input_table}").filter(col("date") == run_date)
transformed_df.printSchema()

result_df = transformed_df.select(
            "ticker",
            create_map(
                lit("open"), round(col("open"), 2),
                lit("close"), round(col("close"), 2),
                lit("low"), round(col("low"), 2),
                lit("high"), round(col("high"), 2),
                lit("volume"), col("volume"),
                lit("transactions"), col("transactions").cast(DoubleType())
            ).alias("aggregates"),
            col("date"))

result_df.writeTo(output_table).option('branch', f"{branch}").overwritePartitions()



