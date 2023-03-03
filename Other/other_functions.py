# Databricks notebook source
from pyspark.sql import DataFrame as SparkDataFrame
from datetime import datetime, time, timedelta

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
