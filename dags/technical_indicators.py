from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta

from airflow.models import Variable

import os

from include.aws.glue_job_submission import create_glue_job
from include.utils.check_holidays import check_holiday, check_previous_day

@dag(
        dag_id = "technical_indicators_dag",    
        description = "Daily dag that fetches stocks data from polygon API and transforms it to the data warehouse",
        start_date = datetime(2025, 4, 2),
        schedule_interval = "30 4 * * 1-5",
        catchup = True,
        max_active_runs = 1,
        tags = ["daily_run"],
        default_args = {
            "owner" : "monk_dude",
            "execution_timeout": timedelta(hours=1),
            "retries": 2,
            "retry_delay": timedelta(minutes=2),
            "retry_exponential_backoff": True
        },
        user_defined_macros = {
            "prev_trade_day" : check_previous_day
        }
)

def technical_indicators_dag():

    common_kwargs = {
       "s3_bucket" : Variable.get("AWS_S3_BUCKET_TABULAR"),
        "catalog_name" : Variable.get("CATALOG_NAME"),
        "tabular_credential" : Variable.get("TABULAR_CREDENTIAL"),
        "aws_access_key_id" : Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key" : Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY"),
        "aws_region" : Variable.get("AWS_GLUE_REGION"),
        "polygon_credentials" : Variable.get('POLYGON_CREDENTIALS')
    }

    exponential_moving_avg_path = os.path.join("include", "eczachly/scripts/transformation/data_marts/exponential_moving_avg.py")
    macd_crossover_path = os.path.join("include", "eczachly/scripts/transformation/data_marts/macd_crossover.py")
    annualised_volatality_path = os.path.join("include", "eczachly/scripts/transformation/data_marts/annualised_volatality.py")

    """
    this task checks if the market is open on the run date.
    """
    check_if_market_is_open = PythonOperator(
        task_id = 'check_if_market_is_open',
        python_callable = check_holiday,
        op_kwargs = { 'run_date' : '{{ ds }}' }
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
                "--input_table" : "monishk37608.dm_exponential_moving_averages",
                "--output_table" : "monishk37608.dm_macd_crossover",
                "--last_run_date" : "{{ prev_trade_day(ds) }}"
            }
        },
        depends_on_past = True,
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
                    "--input_table" : "monk_data_warehouse.fct_daily_stock_prices",
                    "--output_table" : "monishk37608.dm_annualised_volatility",
                    "--last_run_date" : "{{ prev_trade_day(ds) }}"
                }
            },
            depends_on_past = True,
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
                    "--input_table" : "monk_data_warehouse.fct_daily_stock_prices",
                    "--output_table" : "monishk37608.dm_exponential_moving_averages",
                    "--last_run_date" : "{{ prev_trade_day(ds) }}"
                }
            },
            depends_on_past = True,
            provide_context = True
        )

        [annualised_volatality, exponential_moving_avg]

    check_if_market_is_open >> technical_indicators_calculations >> macd_crossover

technical_indicators_dag()