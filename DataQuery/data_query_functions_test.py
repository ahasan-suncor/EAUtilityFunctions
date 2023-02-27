# Databricks notebook source
import unittest
import pandas as pd
from datetime import datetime
from pandas.testing import assert_series_equal

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

class PivotPrecipitationDataTests(unittest.TestCase):
    
    def setUp(self):
        self.pandas_df_test = pd.DataFrame({'DateTimeUTC': [datetime(2022, 1, 1, 12, 7, 21), datetime(2022, 1, 1, 13, 8, 21), datetime(2022, 1, 2, 15, 9, 21)
                                              , datetime(2022, 1, 2, 18, 3, 21), datetime(2022, 2, 1, 19, 7, 21), datetime(2022, 2, 1, 19, 8, 21)]
                              , 'StationName': ['PINCHER CREEK', 'FORT MCMURRAY CS', 'RED DEER REGIONAL A', 'EDMONTON BLATCHFORD', 'CALGARY INTL A', 'LETHBRIDGE A']
                              , 'PrecipAmountmm': [10, 20, 15, 25, 30, 40]})
        self.pandas_df_test.set_index('DateTimeUTC', inplace = True)
        
    def test_pivot_precipitation_data_monthly(self):
        pandas_series_actual = pivot_precipitation_data(self.pandas_df_test, resample_frequency = 'M').mean()
        pandas_series_actual = pandas_series_actual.reset_index(drop = True)
        expected_data = {'CALGARY INTL A': 30.0
                       , 'EDMONTON BLATCHFORD': 25.0
                       , 'FORT MCMURRAY CS': 20.0
                       , 'LETHBRIDGE A': 40.0
                       , 'PINCHER CREEK': 10.0
                       , 'RED DEER REGIONAL A': 15.0
                        }
        pandas_series_expected = pd.Series(expected_data)
        pandas_series_expected = pandas_series_expected.reset_index(drop = True)
                
        assert_series_equal(pandas_series_actual, pandas_series_expected)
        
#     def test_pivot_precipitation_data_with_empty_df(self):
#         pandas_df_empty = pd.DataFrame()
#         pandas_df_actual_output = pivot_precipitation_data(pandas_df_empty)
#         display(pandas_df_actual_output)
#         self.assertTrue(pandas_df_actual_output.empty)
