from datetime import datetime, timedelta
from airflow.exceptions import AirflowSkipException
import holidays

def check_holiday(run_date):
    execution_date = datetime.strptime(run_date, "%Y-%m-%d")
    nyse_holidays = holidays.NYSE()
    execution_day = execution_date.weekday()

    if execution_day >= 5 or execution_date in nyse_holidays:
        raise AirflowSkipException("Market is closed on this date")
    

def check_previous_day(run_date):
    current = datetime.strptime(run_date, "%Y-%m-%d").date() - timedelta(days=1)
    nyse_holidays = holidays.NYSE()

    while current.weekday() >= 5 or current in nyse_holidays:
        current -= timedelta(days=1)

    return current.strftime("%Y-%m-%d")