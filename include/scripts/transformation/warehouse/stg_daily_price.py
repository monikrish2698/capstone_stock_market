from include.aws.glue_job_init import init_glue_job

from pyspark.sql.functions import *
from pyspark.sql.types import *

args, spark, glueContext = init_glue_job(["output_table", "ds", "JOB_NAME", "input_table", "branch"])

output_table = args['output_table']
run_date = args['ds']
input_table = args['input_table']
branch = args['branch']

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table}
    USING iceberg
    PARTITIONED BY (date)
    TBLPROPERTIES (
        'write.wap.enabled' = 'true'
    ) AS
    SELECT
        CAST(null as STRING) as ticker,
        CAST(null as MAP<STRING, DOUBLE>) as aggregates,
        CAST('{run_date.date()}' as DATE) as date
"""

spark.sql(create_table_query)

source_df = spark.table(f"{input_table}").filter(col("date") == run_date)
source_df.printSchema()

result_df = source_df.select(
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

