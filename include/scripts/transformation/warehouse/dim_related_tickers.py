from include.aws.glue_job_init import init_glue_job

from pyspark.sql.functions import *
from pyspark.sql.types import *

args, spark, glueContext = init_glue_job(["output_table", "ds", "JOB_NAME", "input_table"])

output_table = args['output_table']
run_date = args['ds']
input_table = args['input_table']

create_table_query = f"""
    create table if not exists {output_table} (
        ticker STRING,
        related_tickers ARRAY<STRING>,
        date DATE
    )
    USING iceberg
    PARTITIONED BY (date)
"""
spark.sql(create_table_query)

related_tickers_query = f"""
    select ticker, related_tickers, CAST('{run_date.date()}' as DATE) as date from {input_table}
"""

spark.sql(related_tickers_query).writeTo(output_table).overwritePartitions()
