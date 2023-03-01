# Databricks notebook source
pip install scikit-learn

# COMMAND ----------

import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score

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
    
    metrics =  {'f1_score': f1
              , 'precision': precision
              , 'recall': recall
               }
    
    return metrics
