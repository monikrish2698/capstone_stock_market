from airflow.decorators import dag
from airflow.operators.python import PythonOperator

from datetime import datetime

import os

from include.aws.glue_job_submission import create_glue_job
from include.utils.get_common_config_values import common_kwargs

@dag(
    description = "Daily dag that fetches news data from polygon API and transforms it to the data warehouse",
    start_date = datetime(2025, 1, 1),
    schedule = "@daily",
    catchup = True,
    max_active_runs = 3,
    tags = ["daily_run"]
)

def fetch_daily_news():
    
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

fetch_daily_news()