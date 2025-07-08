from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

import json
import sys
from datetime import datetime, timedelta

def init_glue_job(required_args):

    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    spark = glueContext.spark_session

    args = getResolvedOptions(sys.argv, required_args)

    if 'ds' in args:
        args['ds'] = datetime.strptime(args['ds'], '%Y-%m-%d')

    if 'last_run_date' in args:
        args['last_run_date'] = datetime.strptime(args['last_run_date'], '%Y-%m-%d')

    if 'output_table' in args:
        args['output_table'] = args['output_table']

    if 'polygon_credentials' in args:
        args['polygon_credentials'] = json.loads(args['polygon_credentials'])
    
    if 'input_table' in args:
        args['input_table'] = args['input_table']
    
    if 'base_url' in args:
        args['base_url'] = args['base_url']
    
    if 'branch' in args:
        args['branch'] = args['branch']
    
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)  

    return args, spark, glueContext