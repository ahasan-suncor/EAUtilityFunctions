# Databricks notebook source
import pandas as pd
import numpy as np
from sklearn.metrics import f1_score, precision_score, recall_score
from sklearn.metrics import mean_absolute_error, mean_squared_error

# NOTE: !!!!!!!!THIS FUNCTION WAS RENAMED FROM "util_classification_metrics" TO BE MORE ACCURATE AND DESCRIPTIVE!!!!!
def calculate_classification_metrics(pandas_df: pd.DataFrame, class_weight: str = 'macro') -> dict:
    """ 
    Calculates classification metrics (F1 score, precision, recall) between predicted and actual value columns.

    Args:
        pandas_df: Pandas DataFrame containing observed values in 'actual' column and predicted values in 'ypred' column.
        class_weight: Type of classification problem. Valid options: 'binary', 'micro', 'macro', or 'weighted'.

    Returns:
        dict: A dictionary containing the computed metrics.
        
    Assumptions:
        pandas_df contains columns 'actual' and 'ypred', where actual contains the true observed values and ypred contains the predicted values.
        Default to 'macro' averaging for multiclass classification problems.
    """
    
    y_actual = pandas_df['actual']
    y_pred = pandas_df['ypred']
    
    f1 = f1_score(y_actual, y_pred, average = class_weight)
    precision = precision_score(y_actual, y_pred, average = class_weight)
    recall = recall_score(y_actual, y_pred, average = class_weight)
    
    metrics = {'f1_score': f1
             , 'precision': precision
             , 'recall': recall
              }
    
    return metrics

# NOTE: !!!!!!!!THIS FUNCTION WAS RENAMED FROM "util_regression_metrics" TO BE MORE ACCURATE AND DESCRIPTIVE!!!!!
def calculate_regression_metrics(pandas_df: pd.DataFrame) -> dict:
    """ 
    Compute metrics (MAPE, MAE and RMSE) between predicted and actual value columns.

    Args:
        pandas_df: Pandas DataFrame containing observed values in 'actual' column and predicted values in 'ypred' column.

    Returns:
        dict: A dictionary containing the computed metrics.
        
    Assumptions:
        pandas_df contains columns 'actual' and 'ypred', where actual contains the true observed values and ypred contains the predicted values.
    """

    y_actual = pandas_df['actual']
    y_pred = pandas_df['ypred']

    mape = np.mean(np.abs((y_actual - y_pred) / y_actual))
    mae = mean_absolute_error(y_actual, y_pred)
    rmse = mean_squared_error(y_actual, y_pred, squared = True)

    metrics = {'MAPE': mape
             , 'MAE': mae
             , 'RMSE': rmse
             }

    return metrics
