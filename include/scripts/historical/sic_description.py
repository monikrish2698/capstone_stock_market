from include.aws.glue_job_init import init_glue_job
from pyspark.sql.functions import col

args, spark, glueContext = init_glue_job(["JOB_NAME", "output_table"])


sic_description = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("s3://zachwilsonsorganization-522/monk-data/data/sic_descriptions.csv")
)

sic_description = sic_description.withColumnRenamed("clean_title", "sic_description").withColumnRenamed("sic code", "sic_code")

sic_description = sic_description.select("sic_code", "sic_description")

sic_description.writeTo(args["output_table"]).createOrReplace()