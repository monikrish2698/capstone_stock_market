from include.aws.aws_secret_manager import get_secret
from include.aws.glue_job_submission import create_glue_job
import os
from dotenv import load_dotenv
load_dotenv()

schema = os.getenv("HISTORY_SCHEMA")

def create_and_run_glue_job(job_name, script_path, arguments, Variables = None):
    s3_bucket = get_secret("AWS_S3_BUCKET_TABULAR")
    tabular_credential = get_secret("TABULAR_CREDENTIAL")
    catalog_name = get_secret("CATALOG_NAME")  # "eczachly-academy-warehouse"
    aws_region = get_secret("AWS_GLUE_REGION")  # "us-west-2"
    aws_access_key_id = get_secret("DATAEXPERT_AWS_ACCESS_KEY_ID")
    aws_secret_access_key = get_secret("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
    polygon_credentials = get_secret("POLYGON_CREDENTIALS")

    glue_job = create_glue_job(
                    job_name=job_name,
                    script_path=script_path,
                    arguments=arguments,
                    aws_region=aws_region,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    tabular_credential=tabular_credential,
                    s3_bucket=s3_bucket,
                    catalog_name=catalog_name,
                    polygon_credentials=polygon_credentials,
                    description="Glue job to load historical data"
                )
    
local_script_path = os.path.join("include", 'scripts/ingest_polygon_daily_aggregate_historical.py')
create_and_run_glue_job(f'historical_stocks_ingestion_monk', 
                        script_path=local_script_path,
                        arguments={ '--output_table' : f'{schema}.stocks_historical_prices' } )