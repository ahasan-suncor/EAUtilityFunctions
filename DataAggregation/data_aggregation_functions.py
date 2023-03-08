# Databricks notebook source
import pandas as pd
from typing import List

def pd_resamp_mean(pandas_df: pd.DataFrame, column_names: List[str], aggregation_freq: str) -> pd.DataFrame:
    """
    Aggregates pandas dataframe to the specified aggregation level and returns the aggregated mean value for the given frequency in a pandas dataframe.
    aggregation_freq can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    
    Args: 
        pandas_df : A Pandas DataFrame to be aggregated
        column_names : List of column names to apply aggregation to
        aggregation_freq : String indicating the type of aggregation {'H', 'D', 'M', Y'}, it can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    Returns:
        pandas_df_resampled_with_mean : Pandas dataframe after aggregation with mean value
        
    Assumptions:
        The input dataframe has 'Date' column as the index and its type is datetime. The remaining columns in the input dataframe are float or numeric. 
    
    """
    pandas_df_resampled_with_mean  = pandas_df[column_names].resample(aggregation_freq).mean()
    
    return pandas_df_resampled_with_mean


def pd_resamp_median(pandas_df: pd.DataFrame, column_names: List[str], aggregation_freq: str) -> pd.DataFrame:
    """
    Aggregates pandas dataframe to the specified aggregation level and returns the aggregated median value for the given frequency in a pandas dataframe.
    aggregation_freq can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    
    Args: 
        pandas_df : A Pandas DataFrame to be aggregated
        column_names : List of column names to apply aggregation to
        aggregation_freq : String indicating the type of aggregation {'H', 'D', 'M', Y'}, it can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    Returns:
        pandas_df_resampled_with_median : Pandas dataframe after aggregation with median value
        
    Assumptions:
        The input dataframe has 'Date' column as the index and its type is datetime. The remaining columns in the input dataframe are float or numeric. 
    
    """
    pandas_df_resampled_with_median  = pandas_df[column_names].resample(aggregation_freq).median()
    
    return pandas_df_resampled_with_median

def pd_resamp_lastvalue(pandas_df: pd.DataFrame, column_names: List[str], aggregation_freq: str) -> pd.DataFrame:
    """
    Aggregates pandas dataframe to the specified aggregation level and returns the aggregated last value for the given frequency in a pandas dataframe.
    aggregation_freq can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    
    Args: 
        pandas_df : A Pandas DataFrame to be aggregated
        column_names : List of column names to apply aggregation to
        aggregation_freq : String indicating the type of aggregation {'H', 'D', 'M', Y'}, it can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    Returns:
        pandas_df_resampled_with_last : Pandas dataframe after aggregation with last value
        
    Assumptions:
        The input dataframe has 'Date' column as the index and its type is datetime. The remaining columns in the input dataframe are float or numeric. 
    
    """
    pandas_df_resampled_with_last  = pandas_df[column_names].resample(aggregation_freq).last()
    
    return pandas_df_resampled_with_last
  
def pd_resamp_mean_by_status(pandas_df: pd.DataFrame, coldict: dict, aggregation_freq: str) -> pd.DataFrame:
    """
    Aggregates pandas dataframe to the specified aggregation level and returns the aggregated mean value for the given frequency using their respective status column values as a pandas dataframe.
    aggregation_freq can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    The aggregation will return mean value for the given frequency, ignoring times when status NULL and including times when status>=0.
    
    Args: 
        pandas_df : A Pandas DataFrame to be aggregated
        coldict : Dictionary containing column names to aggregate and corresponding status column names 
        aggregation_freq : String indicating the type of aggregation {'H', 'D', 'M', Y'}, it can be 'H' for hourly,'D' for daily or 'M' for monthly or 'Y' for yearly.
    Returns:
        pandas_df_resampled_with_mean_by_status : Pandas dataframe after aggregation with mean value
        
    Assumptions:
        The input dataframe is interpolated and indexed by time. The remaining columns in the input dataframe are float or numeric.
        Status columns indicate whether the corresponding flow's valve was open or not, so status=1 implies valve was open and Status=0 implies valve was close. Null status values are ignored. 
    
    """
    
    pandas_df[list(coldict.keys())] = pandas_df[list(coldict.keys())].mul(pandas_df[list(coldict.values())].values, axis=0)
    
    pandas_df_resampled_with_mean_by_status  = pandas_df[list(coldict.keys())].resample(aggregation_freq).mean()
    
    return pandas_df_resampled_with_mean_by_status
