# Databricks notebook source
import unittest
import pandas as pd
from datetime import datetime

class SelectDataByDateRangeTests(unittest.TestCase):

    def setUp(self):
        self.pandas_df_test = pd.DataFrame({'Category': ['A', 'B', 'A', 'B', 'A']
                                          , 'Value': [1, 2, 3, 4, 5]}
                                          , index = pd.date_range(start = '2022-01-01', end = '2022-01-05'))

    def test_select_data_by_date_range_match(self):
        start_date = datetime(2022, 1, 2)
        end_date = datetime(2022, 1, 4)
        pandas_df_filtered_actual = select_data_by_date_range(self.pandas_df_test, [start_date, end_date])
        self.assertEqual(pandas_df_filtered_actual.index.min(), start_date)
        self.assertEqual(pandas_df_filtered_actual.index.max(), end_date)
        self.assertEqual(pandas_df_filtered_actual.shape, (3, 2))
        self.assertEqual(pandas_df_filtered_actual['Value'].sum(), 9)

    def test_select_data_by_date_range_no_match(self):
        start_date = datetime(2022, 1, 6)
        end_date = datetime(2022, 1, 7)
        pandas_df_filtered_actual = select_data_by_date_range(self.pandas_df_test, [start_date, end_date])
        self.assertTrue(pandas_df_filtered_actual.empty)

    def test_select_data_by_date_range_invalid_input(self):
        start_date = datetime(2022, 1, 4)
        end_date = datetime(2022, 1, 2)
        pandas_df_filtered_actual = select_data_by_date_range(self.pandas_df_test, [start_date, end_date])
        self.assertTrue(pandas_df_filtered_actual.empty)
