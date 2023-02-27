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
