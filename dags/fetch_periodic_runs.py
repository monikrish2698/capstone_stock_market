from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime

import os

from include.aws.glue_job_submission import create_glue_job
from include.utils.get_common_config_values import common_kwargs

@dag(
    description = "Dag that fetch dimensional data from polygon API and transforms it to the data lake",
    schedule = None,
    start_date = datetime(2025, 1, 1),
    catchup = False,
    max_active_runs = 1,    
    tags = ["periodic_run"]
)

def fetch_periodic_runs():
    
    load_related_tickers_path = os.path.join("include", "scripts/dimensional_run/ingest_related_tickers.py")
    load_ticker_types_path = os.path.join("include", "scripts/dimensional_run/ingest_ticker_types.py")
    load_ticker_overview_path = os.path.join("include", "scripts/dimensional_run/ingest_tickers_overview.py")
    load_tickers_path = os.path.join("include", "scripts/dimensional_run/ingest_tickers.py")

    dim_related_tickers_path = os.path.join("include", "scripts/transformation/warehouse/dim_related_tickers.py")
    dim_tickers_path = os.path.join("include", "scripts/transformation/warehouse/dim_tickers.py")

    with TaskGroup(group_id = "extract_dimensional_data") as extract_dimensional_data:
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
        [load_related_tickers, load_ticker_types, load_ticker_overview, load_tickers]

    with TaskGroup(group_id = "transform_dimensional_data") as transform_dimensional_data:
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

        dim_tickers = PythonOperator(
            task_id = "dim_tickers",
            python_callable = create_glue_job,
            op_kwargs = {
                **common_kwargs,
                "job_name" : "dim_tickers",
                "script_path" : dim_tickers_path,
                "arguments" : {
                    "--ds" : "{{ ds }}",
                    "--input_table" : "monk_data_lake.all_tickers",
                    "--output_table" : "monk_data_warehouse.dim_tickers",
                    "--overview" : "monk_data_lake.overview",
                    "--sic_description" : "monk_data_lake.sic_description",
                    "--ticker_types" : "monk_data_lake.all_ticker_types"
                }
            },
            provide_context = True
        )
        [dim_related_tickers, dim_tickers]

    extract_dimensional_data >> transform_dimensional_data

fetch_periodic_runs()