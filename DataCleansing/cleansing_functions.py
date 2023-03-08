# Databricks notebook source
import pyspark.sql.functions as psf
from pyspark.sql.functions import col, lower, first, when, unix_timestamp, from_unixtime, pandas_udf, PandasUDFType
from pyspark.sql import DataFrame as SparkDataFrame
from typing import Dict, List
import pandas as pd
import numpy as np

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
  
def type_cast_tag_data(spark_df: SparkDataFrame, tags_data_type_dict: Dict[str, str]) -> SparkDataFrame:
    """
    Type casts the columns of a Spark DataFrame according to the specified data types for each column.

    Args:
        spark_df: A Spark DataFrame containing the data to type cast.
        tags_data_type_dict: A dictionary where the keys are the column names (tags) to type-cast 
                             and the values are the corresponding data types to cast the columns to.
                             The data types must be in the format supported by PySpark's `cast()` method,
                             e.g. 'integer', 'double', 'timestamp', 'date', 'boolean', etc.

    Returns:
        DataFrame: A new Spark DataFrame with the columns type casted according to the specified data types.
    """

    for tag, data_type in tags_data_type_dict.items():
        if tag in spark_df.columns:
            spark_df = spark_df.withColumn(tag, col(tag).cast(data_type))
    return spark_df

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
  
def impute_process_data(spark_df: SparkDataFrame, tag_dict: SparkDataFrame, timeseries_column_name: str = 'timestamp') -> SparkDataFrame:
    """
    Imputes the null values in a dataframe based on imputation methods (backfill, forwardfill and linear interpolation)
    and the imputation window (in number of rows) specified in the tag dictionary for each tag.

    Args:
        spark_df: The Spark DataFrame to be imputed.
        timestamp_column: The name of the column containing the timestamp data.
        tag_dict: The Spark Data Frame with tag, imputation method and imputation window as columns.

    Returns:
        SparkDataFrame: The Spark DataFrame with null values being imputed.
        
    Assumptions:
        imputation_method and imputation_window are specified in tag_dict for each tag.
    """

    @pandas_udf(spark_df.schema, PandasUDFType.GROUPED_MAP)
    def impute_missing_interpolate(pdf):
        pdf = pdf.sort_values(timeseries_column_name).reset_index(drop=True)
        pdf[tag_name] = pdf[tag_name].interpolate(method='linear', limit=imputation_window, limit_direction='forward')
        return pdf
        

    @pandas_udf(spark_df.schema, PandasUDFType.GROUPED_MAP)
    def impute_missing_ffill(pdf):
        pdf = pdf.sort_values(timeseries_column_name).reset_index(drop=True)
        pdf[tag_name] = pdf[tag_name].fillna(method='ffill', limit=imputation_window)
        return pdf

    @pandas_udf(spark_df.schema, PandasUDFType.GROUPED_MAP)
    def impute_missing_bfill(pdf):
        pdf = pdf.sort_values(timeseries_column_name).reset_index(drop=True)
        pdf[tag_name] = pdf[tag_name].fillna(method='bfill', limit=imputation_window)
        return pdf
   

    tags = tag_dict.select(
        'tag', 'imputation_method', col('imputation_window').cast('int')
    ).toPandas()


    col_list = list(spark_df.columns)
    spark_df = spark_df.orderBy(timeseries_column_name)
    for i in range(len(tags)):
        tag_name = tags.loc[i, 'tag']
        imputation_method = tags.loc[i, 'imputation_method']
        imputation_window = tags.loc[i, 'imputation_window']

        if tag_name in col_list:
            if imputation_method == 'Linear interpolation':
                spark_df = spark_df.groupBy().apply(impute_missing_interpolate)
            elif imputation_method == 'backfill':
                spark_df = spark_df.groupBy().apply(impute_missing_bfill)
            elif imputation_method == 'forward fill':
                spark_df = spark_df.groupBy().apply(impute_missing_ffill)
            else:
                continue
                  
    return spark_df

def rollup_and_agg_process_data_x_min(spark_df: SparkDataFrame, tag_agg_dict: Dict[str, str], interval_minutes: int = 60, timestamp_column_name: str = 'timestamp') -> SparkDataFrame:
    """
    Groups by and aggregates the data for each tag column in a Spark DataFrame.

    Args:
        spark_df: The Spark DataFrame to group by and aggregate.
        tag_agg_dict: A dictionary with tag column names as keys and aggregation functions as values.
        interval_minutes: The time interval in minutes to group by.
        timestamp_column_name: The name of the timestamp column.

    Returns:
        SparkDataFrame: The Spark DataFrame with the data grouped by and aggregated.

    Assumptions:
        The column names in tag_agg_dict are present in the spark_df.
        The aggregate functions in tag_agg_dict are valid aggreagate functions found in pyspark.sql.functions.
    """

    spark_df = spark_df.withColumn('unix_timestamp', unix_timestamp(col(timestamp_column_name)))

    unix_timestamp_rounded_nearest_interval_column = spark_df.unix_timestamp - spark_df.unix_timestamp % (interval_minutes * 60)
    time_interval_column = from_unixtime(unix_timestamp_rounded_nearest_interval_column, 'yyyy-MM-dd HH:mm:ss').alias(timestamp_column_name)

    # Create a list of tag columns and their corresponding aggregation functions.
    tag_agg_column = [getattr(psf, agg_func)(col(tag_column_name)).alias(tag_column_name) for tag_column_name, agg_func in tag_agg_dict.items()]

    spark_df = spark_df.groupBy(time_interval_column) \
                       .agg(*tag_agg_column) \
                       .drop("unix_timestamp")

    return spark_df
  
# FUNCTION WAS RENAMED FROM 'remove_values' AND THE PARAMETERS WERE CHANGED. THIS IS THE SAME AS "clean_process_data_with_outliers" BUT FOR A PANDAS DF????
def replace_range_outliers_with_null(pandas_df: pd.DataFrame, outliers_info_dict: Dict[str, List[float]] = {}) -> pd.DataFrame:
    """
    Replaces values outside the specified range with NULLs for specified columns and returns the filtered DataFrame.
    
    Args:
        pandas_df: Input pandas dataframe
        outliers_info_dict: Dictionary containing outlier ranges for each column.
                            The keys are column names.
                            The values are a list containing minimum and maximum ranges.
        
    Returns:
        DataFrame: Pandas DataFrame after replacing values below the lower range and above the upper range with nulls.
        
    Assumptions:
        The values in outliers_info_dict are [min, max] where min and max are inclusive.
    """
    
    for col_name, (min_val, max_val) in outliers_info_dict.items():
        pandas_df[col_name] = pandas_df[col_name].where((pandas_df[col_name] >= min_val) & (pandas_df[col_name] <= max_val))
    
    return pandas_df

def fill_nan(df:pd.DataFrame, column:List, method:str, limit_max_nulls:int = None):
    """
    Imputes the null values in a dataframe for the columns specified in the column list based on the specified method.
    Limit for the maximum number of null values to be imputed can be specified.
    
    Args: 
        df: Pandas dataframe with datetime as Index
        column: list of column names
        method: imputation methods {'bfill', 'pad', 'ffill', 'interpolate', 'time'}
        limit_max_nulls: maximum number of null samples to be imputed
    
    Returns:
        df: Pandas dataframe with null values being imputed
    """

    df = df.sort_index()
    if limit_max_nulls == None:
       df[column] = df[column].interpolate(method=method)
    else:
       df[column] = df[column].interpolate(method=method, limit=limit_max_nulls)

    return df

# FUNCTION WAS RENAMED FROM 'identify_outliers'
def add_percentile_outlier_flag(pandas_df: pd.DataFrame, column_names: List[str], percentile: float) -> pd.DataFrame:
    """
    Add a column to the dataframe identifying values that fall outside of a certain percentile range.
    
    Args:
        pandas_df: The input dataframe.
        column_names: A list of column names to flag outliers in.
        percentile: The percentile value to use as the threshold for identifying outliers.
    
    Returns:
        DataFrame: A Pandas DataFrame with an additional column '_is_outlier_flag' that indicates if each row is an outlier (Y) or not (N).
    """

    for column in column_names:
        col_data = pandas_df[column]
        # Calculate the lower and upper bounds for the specified percentile parameter for the data in the column.
        lower_bound, upper_bound = col_data.quantile([1 - percentile, percentile])
        pandas_df[column + '_is_outlier_flag'] = np.where((col_data > upper_bound) | (col_data < lower_bound), 'Y', 'N')
        
    return pandas_df

def replace_percentile_outliers_with_null(pandas_df: pd.DataFrame, column_names: List[str], percentile: float) -> pd.DataFrame:
    """
    Replaces outlier values in specified columns of a dataframe with null values based on the percentile provided,
    
    Args:
        pandas_df: The input dataframe.
        column_names: A list of column names to replace outliers in.
        percentile: The percentile value to use as the threshold for identifying outliers.
    
    Returns:
        DataFrame: A Pandas DataFrame with outlier values in specified columns replaced with null values.
    """

    for column in column_names:
        col_data = pandas_df[column]
        # Calculate the lower and upper bounds for the specified percentile parameter for the data in the column.
        lower_bound, upper_bound = col_data.quantile([1 - percentile, percentile])
        pandas_df[column] = col_data.where((col_data <= upper_bound) & (col_data >= lower_bound), None)

    return pandas_df
