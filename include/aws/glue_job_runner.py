from datetime import datetime, timedelta
from include.aws.aws_secret_manager import get_secret
from include.aws.glue_job_submission import create_glue_job
import os
from dotenv import load_dotenv
load_dotenv()

data_lake = os.getenv("DATA_LAKE_SCHEMA")
data_staging = os.getenv("DATA_STAGING_SCHEMA")
data_warehouse = os.getenv("DATA_WAREHOUSE_SCHEMA")

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
    
# local_script_path = os.path.join("include", 'scripts/ingest_polygon_daily_aggregate_historical.py')
# create_and_run_glue_job(f'historical_stocks_ingestion_monk', 
#                         script_path=local_script_path,
#                         arguments={ '--output_table' : f'{schema}.daily_stock_prices' } )

# local_script_path = os.path.join("include", "scripts/ingest_all_tickers.py")
# create_and_run_glue_job(f'fetch_all_tickers',
#     script_path=local_script_path,
#     arguments={'--output_table': f'{schema}.all_tickers', '--ds': '2025-06-21'}
# )

# local_script_path = os.path.join("include", "scripts/ingest_all_ticker_types.py")
# create_and_run_glue_job(f'fetch_all_ticker_types',
#     script_path=local_script_path,
#     arguments={'--output_table': f'{schema}.all_ticker_types', '--ds': '2025-06-21'}
# )

# local_script_path = os.path.join("include", "scripts/ingest_related_tickers.py")
# create_and_run_glue_job(f'fetch_related_tickers',
#     script_path=local_script_path,
#     arguments={'--output_table': f'{schema}.related_tickers', '--ds': '2025-06-21', '--schema': schema}
# )
# print("Related tickers fetched")

# local_script_path = os.path.join("include", "scripts/ingest_overview.py")
# create_and_run_glue_job(f'fetch_overview',
#     script_path=local_script_path,
#     arguments={'--output_table': f'{schema}.overview', '--ds': '2025-06-21', '--schema': schema}
# )
# print("Overview fetched")

# local_script_path = os.path.join("include", "scripts/daily_news_extraction.py")
# create_and_run_glue_job(f'fetch_daily_news',
#     script_path=local_script_path,
#     arguments={'--output_table': f'{schema}.daily_news', '--ds': '2025-06-21'}
# )

# local_script_path = os.path.join("include", "scripts/daily_run/daily_stock_price_extraction.py")
# create_and_run_glue_job(f'daily_stock_price_extraction_2',
#     script_path=local_script_path,
#     arguments={'--output_table': f'{schema}.daily_stock_prices', '--ds': '2025-06-20'}
# )
# print("Overview fetched")

run_date = datetime.strptime("2025-06-20", "%Y-%m-%d")
# local_script_path = os.path.join("include", "scripts/transformation/staging/create_stg_ticker_prices_tbl.py")
# create_and_run_glue_job(f'create_stg_ticker_prices_tbl',
#     script_path=local_script_path,
#     arguments={'--output_table': f'{data_staging}.stg_ticker_prices', '--ds': '2025-06-20'}
# )
# print("staging table created")

# local_script_path = os.path.join("include", "scripts/transformation/staging/stg_ticker_prices.py")
# create_and_run_glue_job(f'stg_ticker_prices',
#     script_path=local_script_path,
#     arguments={
#         '--output_table': f'{data_staging}.stg_ticker_prices', 
#         '--ds': '2025-06-20', 
#         '--schema': data_lake, 
#         '--branch': f'stg_daily_ticker_{run_date.strftime("%Y%m%d")}'
#     }
# )
# print("staging transformation created")


local_script_path = os.path.join("include", "scripts/transformation/warehouse/fct_daily_prices.py")
create_and_run_glue_job(f'fct_daily_prices',
    script_path=local_script_path,
    arguments={
        '--output_table': f'{data_warehouse}.moving_averages_tmp', 
        '--ds': '2025-06-20', 
        '--branch': f'stg_daily_ticker_{run_date.strftime("%Y%m%d")}',
        '--stg_table': f'{data_staging}.stg_ticker_prices'
    }
)