from airflow.decorators import dag
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta

import os

from include.aws.glue_job_submission import create_glue_job
from include.utils.helper_functions import check_holiday
from include.utils.get_common_config_values import common_kwargs

@dag(
    description = "Daily dag that fetches stocks data from polygon API and transforms it to the data warehouse",
    start_date = datetime(2025, 6, 20),
    schedule = "@daily",
    catchup = True,
    max_active_runs = 1,
    tags = ["daily_run"]
)

def get_daily_stock_prices():

    load_daily_stock_prices_path = os.path.join("include", "scripts/daily_run/daily_stock_price.py")

    stg_daily_prices_path = os.path.join("include", "scripts/transformation/warehouse/stg_daily_price.py")
    fct_daily_prices_path = os.path.join("include", "scripts/transformation/warehouse/fct_daily_price.py")


    annualised_volatality_path = os.path.join("include", "scripts/transformation/data_marts/annualised_volatality.py")
    exponential_moving_avg_path = os.path.join("include", "scripts/transformation/data_marts/exponential_moving_avg.py")
    macd_crossover_path = os.path.join("include", "scripts/transformation/data_marts/macd_crossover.py")
    simple_moving_avg_path = os.path.join("include", "scripts/transformation/data_marts/simple_moving_avg.py")

    prices_dq_test_path = os.path.join("include", "scripts/data_quality_test/prices_test.py")

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
                "--branch" : "stg_stock_prices_branch_{{ ds }}"
            }
        },
        provide_context = True
    )
    prices_dq_test = PythonOperator(
        task_id = "prices_dq_test",
        python_callable = create_glue_job,
        op_kwargs = {
            **common_kwargs,
            "job_name" : "prices_dq_test",
            "script_path" : prices_dq_test_path,
            "arguments" : {
                "--ds" : "{{ ds }}",
                "--input_table" : "monk_data_warehouse.fct_daily_prices",
                 "--branch" : "stg_stock_prices_branch_{{ ds }}"
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
                "--branch" : "stg_stock_prices_branch_{{ ds }}"
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
                "--last_run_date" : "{{ (prev_data_interval_end_success.to_date_string() if prev_data_interval_end_success else '1970-01-01')}}"
            }
        },
        provide_context = True
    )



    with TaskGroup(group_id = "technical_indicators_calculations") as technical_indicators_calculations:
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
                    "--last_run_date" : "{{ (prev_data_interval_end_success.to_date_string() if prev_data_interval_end_success else '1970-01-01')}}"
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
                    "--last_run_date" : "{{ (prev_data_interval_end_success.to_date_string() if prev_data_interval_end_success else '1970-01-01') }}"
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
                    "--last_run_date" : "{{ (prev_data_interval_end_success.to_date_string() if prev_data_interval_end_success else '1970-01-01') }}"
                }
            },
            provide_context = True
        )
        [annualised_volatality, exponential_moving_avg, simple_moving_avg]

    check_if_market_is_open >> [skip_daily_run, load_daily_stock_prices]
    load_daily_stock_prices >> stg_daily_stock_prices >> prices_dq_test >> fct_daily_prices >> technical_indicators_calculations >> macd_crossover

get_daily_stock_prices()