# Databricks notebook source
import pandas as pd
from typing import List

def pd_resamp_mean(pandas_df: pd.DataFrame, column_names: List[str], aggregation_frequency: str) -> pd.DataFrame:
#     pandas_df_resampled_with_mean  = pandas_df[column_names].resample(aggregation_frequency).mean()
#     pandas_df_mean = pandas_df_resampled_with_mean.mean()
#     column_means = pandas_df_mean.to_dict()
#     pandas_df_mean = pd.DataFrame(column_means, index = [0])
#     return pandas_df_mean
    # return pandas_df[column_names].mean().to_frame().T
    pass
