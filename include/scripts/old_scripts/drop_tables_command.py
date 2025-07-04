from include.aws.glue_job_init import init_glue_job

args, spark, glueContext = init_glue_job(["JOB_NAME", "output_table"])

output_table = args["output_table"]
# run_date = args["ds"]

# query = f"""
#     CREATE TABLE IF NOT EXISTS {output_table}
#     USING iceberg
#     PARTITIONED BY (date)
#     TBLPROPERTIES (
#         'write.wap.enabled' = 'true'
#     ) AS
#     SELECT
#         CAST(null as STRING) as ticker,
#         CAST(null as MAP<STRING, DOUBLE>) as aggregates,
#         CAST('{run_date}' as DATE) as date
# """

spark.sql(f"DROP TABLE IF EXISTS {output_table}")

# spark.sql(query)