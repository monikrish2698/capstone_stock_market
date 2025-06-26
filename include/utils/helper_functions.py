from pyspark.sql.functions import *
from pyspark.sql.types import *

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