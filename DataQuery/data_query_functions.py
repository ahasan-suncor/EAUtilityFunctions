# Databricks notebook source
from typing import List
import pandas as pd
import numpy as np
import datetime
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, mean_squared_error
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, classification_report, confusion_matrix

# NOTE: !!!!!!!!THIS FUNCTION WAS RENAMED FROM "get_date_range" BECAUSE THATS NOT A ACCURATE AND DESCRIPTIVE NAME!!!!!
def select_data_by_date_range(pandas_df: pd.DataFrame, date_range: List[str]) -> pd.DataFrame:
    """
    Selects data between a specified date range (inclusive) from a Pandas DataFrame with a Datetime Index.

    Args:
        pandas_df: A Pandas DataFrame on whom date selection is required.
        date_range: A list containing the start and end dates in 'YYYY-MM-DD' format.

    Returns:
        A Pandas DataFrame filtered to the specified date range.

    Assumptions:
        The dataframe has index of type TimeStamp.
    """

    start_date, end_date = date_range
    return pandas_df.loc[start_date:end_date]

# NOTE: !!!!!!!!THIS FUNCTION WAS RENAMED FROM "pivot_df" BECAUSE THATS NOT A ACCURATE AND DESCRIPTIVE NAME!!!!!
def pivot_precipitation_data(pandas_df: pd.DataFrame, resample_frequency = 'M'
          , date_column_name = 'DateTimeUTC', group_column_name: str = 'StationName'
          , measure_column_name:str = 'PrecipAmountmm') -> pd.DataFrame:
    """
    Pivot precipitation data so that mean values of monthly precipitation is available for each city.

    Args:
        pandas_df: Input DataFrame with precipitation data.
        date_column_name: Name of date column to use for pivoting.
        group_column_name: Name of column to group by.
        measure_column_name: Name of column containing precipitation amounts.
        resample_frequency: Resampling frequency for the pivot table.
                            Valid values: 'M' for monthly and 'D' for daily.

    Returns:
        A Pandas DataFrame pivoted with mean values of monthly precipitation for each city.

    Assumptions:
        The dataframe has index of type TimeStamp.
    """

    pandas_df_resampled = pandas_df[[group_column_name, measure_column_name]].groupby(group_column_name) \
                                                                             .resample(resample_frequency) \
                                                                             .mean()
    
    pandas_df_pivoted = pd.pivot_table(pandas_df_resampled
                                     , values = measure_column_name
                                     , index = pandas_df_resampled.index
                                     , columns = group_column_name
                                     , aggfunc = 'mean'
                                     , fill_value = None)

    return pandas_df_pivoted
