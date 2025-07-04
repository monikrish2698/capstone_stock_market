from include.aws.glue_job_init import init_glue_job

from pyspark.sql.functions import *
from pyspark.sql.types import *

args, spark, glueContext = init_glue_job(["output_table", "ds", "JOB_NAME", "branch"])

output_table = args['output_table']
run_date = args['ds']
branch = args['branch']

spark.sql(f""" CALL system.fast_forward('{output_table}', 'main', '{branch}') """)
spark.sql(f""" ALTER TABLE {output_table} DROP BRANCH `{branch}`""")