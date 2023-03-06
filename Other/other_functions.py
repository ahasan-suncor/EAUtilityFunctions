# Databricks notebook source
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import desc, asc
from datetime import datetime, date, time, timedelta
from typing import List

def get_shiftid_from_timestamp(timestamp: datetime, day_shift_start_time: time):
    """
    Returns the shiftid corresponding to the given timestamp and day shift start time.

    Args:
        timestamp: The timestamp for which to get the shiftid.
        day_shift_start_time: The start time of the day shift.

    Returns:
        int: The shiftid corresponding to the given timestamp.
    """

    shift_duration_hours = 12

    # Combine today's date (dummy date) with the day shift start time to be able to do timedelta.
    day_shift_start_datetime = datetime.combine(datetime.today(), day_shift_start_time)
    night_shift_start_time = (day_shift_start_datetime + timedelta(hours = shift_duration_hours)).time()

    timestamp_time = timestamp.time()

    is_timestamp_within_current_day = day_shift_start_time <= timestamp_time <= datetime.max.time()
    if is_timestamp_within_current_day:
        shift_datetime = timestamp
    else:
        shift_datetime = timestamp - timedelta(days = 1)

    is_day_shift = day_shift_start_time <= timestamp_time < night_shift_start_time
    shift_identifier = '001' if is_day_shift else '002'

    shiftid = shift_datetime.strftime('%y%m%d') + shift_identifier

    return int(shiftid)

def get_date_range(start_date: date, end_date: date) -> List[date]:
    """
    Returns a list of dates between the start and end date (inclusive).

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.

    Returns:
        list: List of dates between start and end in YYYY-MM-DD format.
    """

    return [start_date + timedelta(n) for n in range(int((end_date - start_date).days) + 1)]

def get_n_rows_by_column(spark_df: SparkDataFrame, column_name_to_sort_by: str, num_of_rows: int, sort_ascending: bool = False) -> SparkDataFrame:
    """
    Returns the top/bottom N rows based on a specified column in the DataFrame.

    Args:
        spark_df: A Spark DataFrame.
        column_name_to_sort_by: The column to sort by.
        num_of_rows: The number of rows to return.
        sort_ascending: Whether to sort in ascending order or not.

    Returns:
        A Spark DataFrame containing the top/bottom N rows.
    """

    if sort_ascending:
        spark_df_sorted = spark_df.orderBy(asc(column_name_to_sort_by))
    else:
        spark_df_sorted = spark_df.orderBy(desc(column_name_to_sort_by))

    return spark_df_sorted.limit(num_of_rows)
