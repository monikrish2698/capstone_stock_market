from airflow.decorators import dag
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from airflow.models import Variable

import os

from include.aws.glue_job_submission import create_glue_job

@dag(
        dag_id = "monk_fetch_daily_news_run_dag",
        description = "Daily dag that fetches news data from polygon API and transforms it to the data warehouse",
        start_date = datetime(2025, 7, 1),
        schedule_interval = "@daily",
        catchup = True,
        max_active_runs = 3,
        tags = ["daily_run"],
        is_paused_upon_creation=False,
        default_args = {
            "owner" : "monk_dude",
            "execution_timeout": timedelta(hours=1),
            "retries": 2,
            "retry_delay": timedelta(minutes=2),
            "retry_exponential_backoff": True
        }
    )

def monk_fetch_daily_news_run_dag():

    common_kwargs = {
       "s3_bucket" : Variable.get("AWS_S3_BUCKET_TABULAR"),
        "catalog_name" : Variable.get("CATALOG_NAME"),
        "tabular_credential" : Variable.get("TABULAR_CREDENTIAL"),
        "aws_access_key_id" : Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key" : Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY"),
        "aws_region" : Variable.get("AWS_GLUE_REGION"),
        "polygon_credentials" : Variable.get('POLYGON_CREDENTIALS')
    }
    
    load_daily_news_path = os.path.join("include", "scripts/daily_run/daily_stock_news.py")
    fct_daily_news_path = os.path.join("include", "scripts/transformation/warehouse/fct_daily_news.py")

    load_daily_news = PythonOperator(
            task_id = 'load_daily_news',
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

    load_daily_news >> fct_daily_news

monk_fetch_daily_news_run_dag()