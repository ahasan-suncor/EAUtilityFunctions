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
