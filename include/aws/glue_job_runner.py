from datetime import datetime, timedelta
from include.aws import upload_to_s3
from include.aws.aws_secret_manager import get_secret
from include.aws.glue_job_submission import create_glue_job
import os
from dotenv import load_dotenv
import holidays
load_dotenv()

data_lake = os.getenv("DATA_LAKE_SCHEMA")
data_staging = os.getenv("DATA_STAGING_SCHEMA")
data_warehouse = os.getenv("DATA_WAREHOUSE_SCHEMA")
data_mart = os.getenv("DATA_MART_SCHEMA")

nyse_holidays = holidays.NYSE()

def create_and_run_glue_job(job_name, script_path, arguments, Variables = None, include_to_s3 = False, include_path_zip = None):
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
                    include_to_s3=include_to_s3,
                    include_path_zip=include_path_zip,
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



include_path_zip = os.path.join(".", "include.zip")
local_script_path = os.path.join("include", "scripts/daily_run/daily_stock_news.py")
create_and_run_glue_job(f'fetch_daily_news',
    script_path=local_script_path,
    include_to_s3=True,
    include_path_zip=include_path_zip,
    arguments={
        '--output_table': f'{data_lake}.daily_news',
        '--base_url': 'https://api.polygon.io/v2/reference/news',
        '--ds': '2025-06-22'}
)

# local_script_path = os.path.join("include", "scripts/periodical/tickers_overview.py")
# create_and_run_glue_job(f'fetch_overview',
#     script_path=local_script_path,
#     arguments={'--output_table': f'{data_mart}.tickers_overview', '--ds': '2025-06-21', '--input_table': f'{data_lake}.all_tickers', '--base_url': "https://api.polygon.io/v3/reference/tickers"}
# )
# print("Overview fetched")


# local_script_path = os.path.join("include", "scripts/daily_run/daily_stock_price.py")
# create_and_run_glue_job(f'daily_stock_price_extraction_2',
#     script_path=local_script_path,
#     arguments={'--output_table': f'{data_lake}.daily_stock_prices', '--ds': '2025-06-23', 
#                '--base_url': "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"}
# )
# print("Overview fetched")


# local_script_path = os.path.join("include", "scripts/transformation/staging/create_stg_ticker_prices_tbl.py")
# create_and_run_glue_job(f'create_stg_ticker_prices_tbl',
#     script_path=local_script_path,
#     arguments={'--output_table': f'{data_staging}.stg_ticker_prices', '--ds': '2025-06-23', '--input_table': f'{data_lake}.daily_stock_prices', '--branch': 'stg_daily_ticker_20250623'}
# )
# print("staging table created")

# start_date = datetime.strptime("2021-01-04", "%Y-%m-%d")
# end_date = datetime.strptime("2021-02-28", "%Y-%m-%d")

# while start_date <= end_date:
#     run_date = start_date
#     execution_day = run_date.weekday()

#     if run_date not in nyse_holidays and execution_day < 5:
#         local_script_path = os.path.join("include", "scripts/transformation/staging/stg_ticker_prices.py")
#         create_and_run_glue_job(f'stg_ticker_prices',
#             script_path=local_script_path,
#             arguments={
#                 '--output_table': f'{data_staging}.stg_ticker_prices', 
#                 '--ds': run_date.strftime("%Y-%m-%d"), 
#                 '--schema': data_lake, 
#                 '--branch': f'stg_daily_ticker_{run_date.strftime("%Y%m%d")}'
#             }
#         )
#         print("staging transformation created")


#         local_script_path = os.path.join("include", "scripts/transformation/warehouse/fct_daily_prices.py")
#         create_and_run_glue_job(f'fct_daily_prices',
#             script_path=local_script_path,
#             arguments={
#                 '--output_table': f'{data_warehouse}.fct_daily_prices', 
#                 '--ds': run_date.strftime("%Y-%m-%d"), 
#                 '--branch': f'stg_daily_ticker_{run_date.strftime("%Y%m%d")}',
#                 '--stg_table': f'{data_staging}.stg_ticker_prices'
#             }
#         )
#         print("processing done for ", run_date.strftime("%Y-%m-%d"));
#     else:
#         print(f"Skipping {run_date.strftime('%Y-%m-%d')} because it is a holiday or weekend")
#     start_date += timedelta(days=1)


# yesterday_date = start_date - timedelta(days=1)

# while start_date <= end_date:
#     run_date = start_date
#     execution_day = run_date.weekday()

#     if run_date not in nyse_holidays and execution_day < 5:
#         local_script_path = os.path.join("include", "scripts/transformation/data_marts/exponential_moving_averages.py")
#         create_and_run_glue_job(f'exponential_moving_averages',
#             script_path=local_script_path,
#             arguments={
#                 '--output_table': f'{data_mart}.exponential_moving_averages', 
#                 '--ds': run_date.strftime("%Y-%m-%d"), 
#                 '--fct_table': f'{data_warehouse}.fct_daily_prices',
#                 '--last_run_date': yesterday_date.strftime("%Y-%m-%d")
#             }
#         )
#         yesterday_date = run_date
#     else:
#         print(f"Skipping {run_date.strftime('%Y-%m-%d')} because it is a holiday or weekend")
#     print("processing done for ", run_date.strftime("%Y-%m-%d"));
#     start_date += timedelta(days=1)

# print("exponential moving averages created")

# local_script_path = os.path.join("include", "scripts/transformation/warehouse/fct_daily_news.py")
# create_and_run_glue_job(f'fct_daily_news',
#     script_path=local_script_path,
#     arguments={'--output_table': f'{data_warehouse}.fct_daily_news', '--ds': '2025-06-21', '--schema': data_lake}
# )
# print("staging table created")


# yesterday_date = start_date - timedelta(days=1)

# while start_date <= end_date:
#     run_date = start_date
#     execution_day = run_date.weekday()

#     if run_date not in nyse_holidays and execution_day < 5:
#         local_script_path = os.path.join("include", "scripts/transformation/data_marts/macd_crossover.py")
#         create_and_run_glue_job(f'macd_crossover',
#             script_path=local_script_path,
#             arguments={
#                 '--output_table': f'{data_mart}.macd_crossover_tmp', 
#                 '--ds': run_date.strftime("%Y-%m-%d"), 
#                 '--ema_table': f'{data_mart}.exponential_moving_averages',
#                 '--last_run_date': yesterday_date.strftime("%Y-%m-%d")
#             }
#         )
#         yesterday_date = run_date
#     else:
#         print(f"Skipping {run_date.strftime('%Y-%m-%d')} because it is a holiday or weekend")
#     print("processing done for ", run_date.strftime("%Y-%m-%d"));
#     start_date += timedelta(days=1)

# print("macd crossover created")



# yesterday_date = start_date - timedelta(days=1)

# while start_date <= end_date:
#     run_date = start_date
#     execution_day = run_date.weekday()

#     if run_date not in nyse_holidays and execution_day < 5:
#         local_script_path = os.path.join("include", "scripts/transformation/data_marts/annualised_volatality.py")
#         create_and_run_glue_job(f'annualised_volatality',
#             script_path=local_script_path,
#             arguments={
#                 '--output_table': f'{data_mart}.annualised_volatality_tmppp', 
#                 '--ds': run_date.strftime("%Y-%m-%d"), 
#                 '--fct_table': f'{data_warehouse}.fct_daily_prices',
#                 '--last_run_date': yesterday_date.strftime("%Y-%m-%d")
#             }
#         )
#         yesterday_date = run_date
#     else:
#         print(f"Skipping {run_date.strftime('%Y-%m-%d')} because it is a holiday or weekend")
#     print("processing done for ", run_date.strftime("%Y-%m-%d"));
#     start_date += timedelta(days=1)

# print("macd crossover created")