from airflow.decorators import dag
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta

import os

from include.aws.glue_job_submission import create_glue_job
from include.aws.aws_secret_manager import get_secret
from include.utils.helper_functions import check_holiday

@dag(
    description = "Daily dag that fetches stocks data from polygon API and transforms it to the data warehouse",
    start_date = datetime(2025, 6, 23),
    schedule = "@daily",
    catchup = True,
    max_active_runs = 1,
    tags = ["daily_run"]
)

def get_daily_stock_prices():

    load_daily_stock_prices_path = os.path.join("include", "scripts/daily_run/daily_stock_price.py")
    load_daily_news_path = os.path.join("include", "scripts/daily_run/daily_stock_news.py")

    load_related_tickers_path = os.path.join("include", "scripts/dimensional_run/ingest_related_tickers.py")
    load_ticker_types_path = os.path.join("include", "scripts/dimensional_run/ingest_ticker_types.py")
    load_ticker_overview_path = os.path.join("include", "scripts/dimensional_run/ingest_tickers_overview.py")
    load_tickers_path = os.path.join("include", "scripts/dimensional_run/ingest_tickers.py")

    stg_daily_prices_path = os.path.join("include", "scripts/transformation/warehouse/stg_daily_price.py")
    fct_daily_prices_path = os.path.join("include", "scripts/transformation/warehouse/fct_daily_price.py")
    fct_daily_news_path = os.path.join("include", "scripts/transformation/warehouse/fct_daily_news.py")
    dim_related_tickers_path = os.path.join("include", "scripts/transformation/warehouse/dim_related_tickers.py")

    annualised_volatality_path = os.path.join("include", "scripts/transformation/data_marts/annualised_volatality.py")
    exponential_moving_avg_path = os.path.join("include", "scripts/transformation/data_marts/exponential_moving_avg.py")
    macd_crossover_path = os.path.join("include", "scripts/transformation/data_marts/macd_crossover.py")
    simple_moving_avg_path = os.path.join("include", "scripts/transformation/data_marts/simple_moving_avg.py")

    common_kwargs = {
        "s3_bucket" : get_secret("AWS_S3_BUCKET_TABULAR"),
        "catalog_name" : get_secret("CATALOG_NAME"),
        "tabular_credential" : get_secret("TABULAR_CREDENTIAL"),
        "aws_access_key_id" : get_secret("DATAEXPERT_AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key" : get_secret("DATAEXPERT_AWS_SECRET_ACCESS_KEY"),
        "aws_region" : get_secret("AWS_GLUE_REGION"),
        "polygon_credentials" : get_secret("POLYGON_CREDENTIALS"),
    }

    """
    this task checks if the market is open on the run date.
    """
    check_if_market_is_open = BranchPythonOperator(
        task_id = 'check_if_market_is_open',
        python_callable = check_holiday,
        op_kwargs = { 'run_date' : '{{ ds }}' }
    )

    """
    this task checks if the rest of daily dag run should be skipped
    """
    skip_daily_run =  EmptyOperator(task_id = 'skip_daily_run')
    
    """
    this task loads the daily stock prices from the polygon API and loads it into the data lake
    """
    load_daily_stock_prices =PythonOperator(
        task_id = "load_daily_stock_prices",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "daily_stock_price_extraction_2",
            "script_path" : load_daily_stock_prices_path,
            "arguments" : {
                "--ds" : "{{ ds }}",
                "--output_table" : "monk_data_lake.daily_stock_prices",
                "--base_url" : "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
            }
        },
        provide_context = True
    )

    load_daily_news = PythonOperator(
        taks_id = 'load_daily_news',
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "fetch_daily_news",
            "script_path" : load_daily_news_path,
            "arguments" : {
                "--base_url" : "https://api.polygon.io/v2/reference/news",
                "--ds" : "{{ ds }}",
                "--output_table" : "monk_data_lake.daily_news"
            } 
        },
        provide_context = True
    )

    stg_daily_stock_prices = PythonOperator(
        task_id = "stg_daily_stock_prices",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "stg_ticker_prices",
            "script_path" : stg_daily_prices_path,
            "arguments" : {
                "--output_table" : "monk_data_warehouse.fct_daily_prices",
                "--ds" : "{{ ds }}",
                "--input_table" : "monk_data_lake.daily_stock_prices",
                "--branch" : f"stg_stock_prices_branch_{{ ds }}"
            }
        },
        provide_context = True
    )

    fct_daily_prices = PythonOperator(
        task_id = "fct_daily_stock_prices",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "fct_daily_prices",
            "script_path" : fct_daily_prices_path,
            "arguments" : {
                "--ds" : "{{ ds }}",
                "--output_table" : "monk_data_warehouse.fct_daily_prices",
                "--branch" : f"stg_stock_prices_branch_{{ ds }}"
            }
        },
        provide_context = True
    )

    fct_daily_news = PythonOperator(
        task_id = "fct_daily_news",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "fct_daily_news",
            "script_path" : fct_daily_news_path,
            "arguments" : {
                "--ds" : "{{ ds }}",
                "--input_table" : "monk_data_lake.daily_news",
                "--output_table" : "monk_data_warehouse.fct_daily_news"
            }
        },
        provide_context = True
    )

    dim_related_tickers = PythonOperator(
        task_id = "dim_related_tickers",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "dim_related_tickers",
            "script_path" : dim_related_tickers_path,
            "arguments" : {
                "--ds" : "{{ ds }}",
                "--input_table" : "monk_data_lake.related_tickers",
                "--output_table" : "monk_data_warehouse.dim_related_tickers"
            }
        },
        provide_context = True
    )
    
    annualised_volatality = PythonOperator(
        task_id = "annualised_volatality",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "annualised_volatality",
            "script_path" : annualised_volatality_path,
            "arguments" : {
                "--ds" : "{{ ds }}",
                "--input_table" : "monk_data_warehouse.fct_daily_prices",
                "--output_table" : "monk_data_mart.annualised_volatality",
                "--last_run_date" : "{{ prev_data_interval_end_success }}"
            }
        },
        provide_context = True
    )

    exponential_moving_avg = PythonOperator(
        task_id = "exponential_moving_avg",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "exponential_moving_averages",
            "script_path" : exponential_moving_avg_path,
            "arguments" : {
                "--ds" : "{{ ds }}",
                "--input_table" : "monk_data_warehouse.fct_daily_prices",
                "--output_table" : "monk_data_mart.exponential_moving_averages",
                "--last_run_date" : "{{ prev_data_interval_end_success }}"
            }
        },
        provide_context = True
    )

    macd_crossover = PythonOperator(
        task_id = "macd_crossover",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "macd_crossover",
            "script_path" : macd_crossover_path,
            "arguments" : {
                "--ds" : "{{ ds }}",
                "--input_table" : "monk_data_mart.exponential_moving_averages",
                "--output_table" : "monk_data_mart.macd_crossover",
                "--last_run_date" : "{{ prev_data_interval_end_success }}"
            }
        },
        provide_context = True
    )

    simple_moving_avg = PythonOperator(
        task_id = "simple_moving_avg",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "simple_moving_avg",
            "script_path" : simple_moving_avg_path,
            "arguments" : {
                "--ds" : "{{ ds }}",
                "--input_table" : "monk_data_warehouse.fct_daily_prices",
                "--output_table" : "monk_data_mart.simple_moving_averages",
                "--last_run_date" : "{{ prev_data_interval_end_success }}"
            }
        },
        provide_context = True
    )

    load_related_tickers = PythonOperator(
        task_id = "load_related_tickers",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "fetch_related_tickers",
            "script_path" : load_related_tickers_path,
            "arguments" : {
                "--ds" : "{{ ds }}",
                "--base_url" : "https://api.polygon.io/v1/related-companies",
                "--output_table" : "monk_data_lake.related_tickers",
                "--input_table" : "monk_data_lake.all_tickers"
            }
        },
        provide_context = True
    )

    load_ticker_types = PythonOperator(
        task_id = "load_ticker_types",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "fetch_all_ticker_types",
            "script_path" : load_ticker_types_path,
            "arguments" : {
                "--ds" : "{{ ds }}",
                "--base_url" : "https://api.polygon.io/v3/reference/tickers/types",
                "--output_table" : "monk_data_lake.all_ticker_types"
            }
        },
        provide_context = True
    )

    load_ticker_overview = PythonOperator(
        task_id = "load_ticker_overview",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "fetch_overview",
            "script_path" : load_ticker_overview_path,
            "arguments" : {
                "--ds" : "{{ ds }}",
                "--base_url" : "https://api.polygon.io/v3/reference/tickers",
                "--output_table" : "monk_data_lake.overview",
                "--input_table" : "monk_data_lake.all_tickers"
            }
        },
        provide_context = True
    )

    load_tickers = PythonOperator(
        task_id = "load_tickers",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "fetch_all_tickers",
            "script_path" : load_tickers_path,
            "arguments" : {
                "--ds" : "{{ ds }}",
                "--base_url" : "https://api.polygon.io/v3/reference/tickers",
                "--output_table" : "monk_data_lake.all_tickers"
            }
        },
        provide_context = True
    )

    load_daily_stock_prices

get_daily_stock_prices()