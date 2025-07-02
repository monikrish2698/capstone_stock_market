from include.aws.glue_job_init import init_glue_job

args, spark, glueContext = init_glue_job(["JOB_NAME", "output_table"])

output_table = args["output_table"]

spark.sql(f"DROP TABLE IF EXISTS {output_table}")