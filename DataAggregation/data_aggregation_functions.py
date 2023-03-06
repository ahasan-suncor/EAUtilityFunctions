# Databricks notebook source
import pandas as pd
from typing import List

def pd_resamp_mean(pandas_df: pd.DataFrame, column_names: List[str], aggregation_frequency: str) -> pd.DataFrame:
    """
    Aggregates pandas dataframe to the specified aggregation level and returns the aggregated mean value for the given frequency in a panas dataframe.
    aggregation_freq can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    
    Args: 
        pandas_df : A Pandas DataFrame to be aggregated
        column_names : List of column names to apply aggregation to
        aggregation_frequency : String indicating the type of aggregation {'H', 'D', 'M', Y'}, it can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    Returns:
        pandas_df_resampled_with_mean : Pandas dataframe after aggregation with mean value
        
    Assumptions:
        The input dataframe has 'Date' column as the index and its type is datetime. The remaining columns in the input dataframe are float or numeric. 
    
    """
    pandas_df_resampled_with_mean  = pandas_df[column_names].resample(aggregation_frequency).mean()
    
    return pandas_df_resampled_with_mean


def pd_resamp_median(pandas_df: pd.DataFrame, column_names: List[str], aggregation_frequency: str) -> pd.DataFrame:
    """
    Aggregates pandas dataframe to the specified aggregation level and returns the aggregated median value for the given frequency in a panas dataframe.
    aggregation_freq can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    
    Args: 
        pandas_df : A Pandas DataFrame to be aggregated
        column_names : List of column names to apply aggregation to
        aggregation_frequency : String indicating the type of aggregation {'H', 'D', 'M', Y'}, it can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    Returns:
        pandas_df_resampled_with_median : Pandas dataframe after aggregation with median value
        
    Assumptions:
        The input dataframe has 'Date' column as the index and its type is datetime. The remaining columns in the input dataframe are float or numeric. 
    
    """
    pandas_df_resampled_with_median  = pandas_df[column_names].resample(aggregation_frequency).median()
    
    return pandas_df_resampled_with_median

def pd_resamp_last(pandas_df: pd.DataFrame, column_names: List[str], aggregation_frequency: str) -> pd.DataFrame:
    """
    Aggregates pandas dataframe to the specified aggregation level and returns the aggregated last value for the given frequency in a panas dataframe.
    aggregation_freq can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    
    Args: 
        pandas_df : A Pandas DataFrame to be aggregated
        column_names : List of column names to apply aggregation to
        aggregation_frequency : String indicating the type of aggregation {'H', 'D', 'M', Y'}, it can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    Returns:
        pandas_df_resampled_with_last : Pandas dataframe after aggregation with last value
        
    Assumptions:
        The input dataframe has 'Date' column as the index and its type is datetime. The remaining columns in the input dataframe are float or numeric. 
    
    """
    pandas_df_resampled_with_last  = pandas_df[column_names].resample(aggregation_frequency).last()
    
    return pandas_df_resampled_with_last
