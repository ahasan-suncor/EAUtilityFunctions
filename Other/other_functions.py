# Databricks notebook source
from pyspark.sql import DataFrame as SparkDataFrame
from datetime import datetime, time, timedelta
from pyspark.sql.functions import md5, concat_ws

def load_data_from_path(data_path: str, data_format: str = 'delta', data_options: dict = {}) -> SparkDataFrame:
    """
    Loads data from a specified path using PySpark.

    Args:
        data_path: A string specifying the path to the data.
        data_format: A string specifying the format of the data.
        data_options: A dictionary of options to pass to the PySpark reader.

    Returns:
        A PySpark DataFrame containing the loaded data.
        Returns None if data_path is None.
    """

    if data_path is None:
        return None

    return spark.read \
                .format(data_format) \
                .options(**data_options) \
                .load(data_path)

def add_column_prefix(spark_df: SparkDataFrame, prefix_str: str) -> SparkDataFrame:
    """
    Adds a prefix string to the names of all columns in a PySpark DataFrame.

    Args:
        spark_df: A PySpark DataFrame.
        prefix_str: A string to add as a prefix to all column names.

    Returns:
        A new PySpark DataFrame with all column names prefixed.
    """

    for df_column in spark_df.columns:
        spark_df = spark_df.withColumnRenamed(df_column, '{}{}'.format(prefix_str, df_column))
    return spark_df

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

def add_audit_cols_to_spark_df(spark_df: SparkDataFrame) -> SparkDataFrame:
    """
    This function adds audit columns to a Spark DataFrame regarding row creation.

    Args:
        spark_df: The Spark DataFrame to add the hash column to.

    Returns:
        SparkDataFrame: The Spark DataFrame with audit columns added
    """

    sdf_audit_cols = spark.sql("""
                               SELECT CURRENT_USER AS CreatedBy
                                    , CURRENT_TIMESTAMP AS CreatedDateTime_UTC
                               """)

    return spark_df.crossJoin(sdf_audit_cols)

def add_business_key_hash_value_to_spark_df(spark_df: SparkDataFrame, business_key_cols: list) -> SparkDataFrame:
    """
    This function adds a new column to a Spark DataFrame that contains a hash value of the business key columns.
    The resulting hash is used to uniquely identify the record based on the business key.

    Args:
        spark_df: The Spark DataFrame to add the hash column to.
        business_key_cols: A list of column names to use as the business key columns.

    Returns:
        SparkDataFrame: The Spark DataFrame with the new hash column added.
    """

    # We drop the duplicates because the key will be a unique identfier for each record.
    values_seperator = ':'
    return spark_df.dropDuplicates(business_key_cols) \
                   .withColumn('BusinessKeyColHash', md5(concat_ws(values_seperator, *business_key_cols)))
