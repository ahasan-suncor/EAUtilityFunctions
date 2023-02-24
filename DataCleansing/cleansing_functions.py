# Databricks notebook source
import pyspark.sql.functions as psf
from pyspark.sql.functions import col, lower, first, when, unix_timestamp, from_unixtime
from pyspark.sql import DataFrame as SparkDataFrame
from typing import Dict, List

def normalize_column_names(spark_df: SparkDataFrame) -> SparkDataFrame:
    """
    Normalizes the column names of a Spark DataFrame by converting all column names to lowercase
    and replacing spaces with underscores.

    Args:
        spark_df: The Spark DataFrame whose column names are to be normalized.

    Returns:
        SparkDataFrame: The Spark DataFrame with normalized column names.
    """

    normalized_columns = [col(col_name).alias(col_name.lower().replace(' ', '_')) for col_name in spark_df.columns]
    normalized_spark_df  = spark_df.select(normalized_columns)
    
    return normalized_spark_df

def rename_columns_after_agg(spark_df: SparkDataFrame) -> SparkDataFrame:
    """
    Renames columns in a Spark DataFrame that were given default names after an aggregation function is applied.

    Args:
        spark_df: The Spark DataFrame whose column names should be normalized.

    Returns:
        SparkDataFrame: The Spark DataFrame with renamed columns after aggregation functions.
    
    Assumptions:
        The column name is either <agg_func_name>(<col_name>) or <col_name>.
    """

    renamed_columns = []
    for col_name in spark_df.columns:
        if '(' in col_name and ')' in col_name:
            # After the split, the column name with ends with a ')'. So we replace that with an empty string.
            new_col_name = col_name.split('(')[-1].replace(')', '')
            renamed_columns.append(col(col_name).alias(new_col_name))
        else:
            renamed_columns.append(col(col_name))

    renamed_spark_df  = spark_df.select(renamed_columns)
    return renamed_spark_df

def filter_priority_process_data(spark_df: SparkDataFrame, priority_tags_dict: Dict[str, List[str]]) -> SparkDataFrame:
    """
    Filters a Spark DataFrame based on a list of priority tags.

    Args:
        spark_df: The Spark DataFrame to filter.
        priority_tags_dict: A dictionary of the tag column name and list.
            - The key is the name of the column containing the tags.
            - The values is the list of priority tags to filter on.

    Returns:
        SparkDataFrame: The Spark DataFrame filtered to include only rows with priority tags.
    """

    tag_column_name = priority_tags_dict['tag_column_name']
    priority_tags = priority_tags_dict['priority_tags']

    return spark_df.filter(col(tag_column_name).isin(priority_tags))

def pivot_process_data(spark_df: SparkDataFrame, pivot_column_name: str, aggregate_column_name: str) -> SparkDataFrame:
    """
    Pivots a column in a Spark DataFrame and returns the result.

    Args:
        spark_df: The PySpark DataFrame to pivot.
        pivot_column_name: The name of the column to pivot on.
        aggregate_column_name: The name of the column to aggregate.

    Returns:
        SparkDataFrame: The Spark DataFrame with the specified column pivoted.

    Assumptions:
        There is only one column being pivoted, and only one column being aggregated.
    """

    fixed_columns = [col_name for col_name in spark_df.columns if col_name not in (pivot_column_name, aggregate_column_name)]

    spark_df_pivoted = spark_df.groupBy(*fixed_columns) \
                               .pivot(pivot_column_name) \
                               .agg(first(col(aggregate_column_name)))

    return spark_df_pivoted

def clean_process_data_with_outliers(spark_df: SparkDataFrame, outliers_info_dict: Dict[str, List[float]]) -> SparkDataFrame:
    """
    Cleans column values in a Spark DataFrame based on outlier ranges and returns the result.

    Args:
        spark_df: The Spark DataFrame to filter.
        outliers_info_dict: A dictionary containing the outlier ranges for each column to filter.
                            The keys are column names.
                            The values are lists containing the minimum and maximum values for each column.

    Returns:
        SparkDataFrame: The filtered Spark DataFrame. NULL if the column is out of range.

    Assumptions: 
        The column names in outliers_info are present in the spark_df.
        The values in outliers_info are [min, max] where min and max are inclusive.
    """

    for col_name, (min_val, max_val) in outliers_info_dict.items():
        spark_df = spark_df.withColumn(col_name, when((col(col_name) >= min_val) 
                                                    & (col(col_name) <= max_val)
                                               , col(col_name)) \
                                                 .otherwise(None))
    return spark_df

def fill_timeseries_x_interval(spark_df: SparkDataFrame, timeseries_column_name: str = 'timestamp', interval_minutes: int = 1) -> SparkDataFrame:
    """
    Fills in gaps in a time series by joining with another dataframe containing all rows for the specified interval.

    Args:
        spark_df: The Spark DataFrame to fill in.
        timestamp_column: The name of the column containing the timestamp data.
        interval_minutes: The interval (in minutes) at which to fill gaps in the time series.

    Returns:
        SparkDataFrame: The Spark DataFrame with missing timestamps filled in with NULL values.
    """

    start_time_unix, end_time_unix = spark_df.selectExpr(f'min(unix_timestamp({timeseries_column_name}))',
                                                         f'max(unix_timestamp({timeseries_column_name}))') \
                                             .first()

    # Generate a DataFrame of evenly spaced timestamps for the specified interval.
    spark_df_expected_timestamps = spark.range(start = start_time_unix
                                             , end = end_time_unix + 60 * interval_minutes # end inclusive
                                             , step = (60 * interval_minutes)) \
                                        .select(from_unixtime(col('id')).alias(timeseries_column_name))

    joined_df = spark_df_expected_timestamps.join(spark_df
                                                , on = spark_df[timeseries_column_name] == spark_df_expected_timestamps[timeseries_column_name]
                                                , how = 'fullouter') \
                                            .drop(spark_df[timeseries_column_name])
                  
    return joined_df
