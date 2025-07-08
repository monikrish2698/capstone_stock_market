from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import holidays

from pyspark.sql import Column

def mean_last_n (col_name: str, count_col : str, n: int) -> Column:
    """
    Calculate the mean of the last n values in a column.
    """
    return (
        when(col(count_col) >= n,
             expr(
                 f"aggregate(slice({col_name}, 1, {n}), cast(0.0 as double), (acc, x) -> acc + x) / {n}"
                 )
                 ).otherwise(lit(None)))

def sum_squared_deviations (col_name: str, count_col : str, mean_col : str, n: int) -> Column:
    """
    Calculate the sum of squared deviations from the mean of the last n values in a column.
    """
    return (
        when(col(count_col) >= n,
             expr(
                 f"aggregate(slice({col_name}, 1, {n}), cast(0.0 as double), (acc, x) -> acc + pow(x - {mean_col}, 2))"
             )
             ).otherwise(lit(None)))

def check_holiday(run_date):
    execution_date = datetime.strptime(run_date, "%Y-%m-%d")
    nyse_holidays = holidays.NYSE()
    execution_day = execution_date.weekday()

    if execution_day >= 5 or execution_date in nyse_holidays:
        return 'skip_daily_run'
    else:
        return 'load_daily_stock_prices'