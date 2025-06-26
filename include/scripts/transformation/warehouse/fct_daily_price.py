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
    PARTITIONED BY (date) AS
    SELECT
        CAST(null as STRING) as ticker,
        CAST(null as MAP<STRING, DOUBLE>) as aggregates,
        CAST('{run_date.date()}' as DATE) as date
"""

spark.sql(create_table_query)

stg_df = spark.read.option("branch", f"{branch}").table(f"{input_table}")
stg_df.writeTo(output_table).overwritePartitions()

delete_branch_query = f"""alter table {input_table} drop branch {branch}"""
spark.sql(delete_branch_query)
