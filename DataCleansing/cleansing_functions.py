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
