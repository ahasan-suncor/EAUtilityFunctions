# Databricks notebook source
import unittest

class Pd_Resamp_Mean_Test(unittest.TestCase):
    
    def test_resamp_mean_hour(self):
        
        pandas_df_test = pd.DataFrame({'Date': ['2022-06-01 6:00', '2022-06-01 6:01', '2022-06-01 6:02'
                                            , '2022-06-01 6:03', '2022-06-01 6:04', '2022-06-01 6:05'
                                            , '2022-06-01 6:06', '2022-06-01 6:07', '2022-06-01 6:08'
                                            , '2022-06-01 7:09', '2022-06-01 7:10', '2022-06-01 7:11'
                                            , '2022-06-01 8:12', '2022-06-01 8:13', '2022-06-01 8:14']
                          , 'tag_flow_1': [45, 56, 100, 8, 9, 63, 59, 56, 41, 8, 8, 53, 46, 44, 57]
                          , 'tag_flow_2': [51, 43, 59, 10, 11, 18, 93, 97, 45, 9, 10, 18, 27, 33, 38]
                                })

        pandas_df_test['Date'] = pd.to_datetime(pandas_df_test['Date'])
        pandas_df_test.set_index('Date',inplace=True)

        columns = ['tag_flow_1', 'tag_flow_2']
        
        pandas_df_actual = pd_resamp_mean(pandas_df_test, columns, 'H')
        

        pandas_df_expected = pd.DataFrame({'Date': ['2022-06-01 6:00', '2022-06-01 7:00', '2022-06-01 8:00']
                                  , 'tag_flow_1': [48.555556, 23.000000, 49.000000]
                                  , 'tag_flow_2': [47.444444, 12.333333, 32.666667]
                                        })
        
        pandas_df_expected['Date'] = pd.to_datetime(pandas_df_expected['Date'])
        pandas_df_expected.set_index('Date',inplace=True)
        pd.testing.assert_frame_equal(pandas_df_actual, pandas_df_expected, check_freq=False)
        
    def test_resamp_mean_day(self):
        
        pandas_df_test = pd.DataFrame({'Date': ['2022-06-01 6:00', '2022-06-01 6:01', '2022-06-01 6:02'
                                            , '2022-06-01 6:03', '2022-06-01 6:04', '2022-06-01 6:05'
                                            , '2022-06-01 6:06', '2022-06-01 6:07', '2022-06-01 6:08'
                                            , '2022-06-02 7:09', '2022-06-02 7:10', '2022-06-02 7:11'
                                            , '2022-06-03 8:12', '2022-06-03 8:13', '2022-06-03 8:14']
                          , 'tag_flow_1': [45, 56, 100, 8, 9, 63, 59, 56, 41, 8, 8, 53, 46, 44, 57]
                          , 'tag_flow_2': [51, 43, 59, 10, 11, 18, 93, 97, 45, 9, 10, 18, 27, 33, 38]
                                })

        pandas_df_test['Date'] = pd.to_datetime(pandas_df_test['Date'])
        pandas_df_test.set_index('Date',inplace=True)

        columns = ['tag_flow_1', 'tag_flow_2']
        
        pandas_df_actual = pd_resamp_mean(pandas_df_test, columns, 'D')

        pandas_df_expected = pd.DataFrame({'Date': ['2022-06-01', '2022-06-02', '2022-06-03']
                                  , 'tag_flow_1': [48.555556, 23.000000, 49.000000]
                                  , 'tag_flow_2': [47.444444, 12.333333, 32.666667]
                                        })

        pandas_df_expected['Date'] = pd.to_datetime(pandas_df_expected['Date'])
        pandas_df_expected.set_index('Date',inplace=True)
        pd.testing.assert_frame_equal(pandas_df_actual, pandas_df_expected, check_freq=False)
        
    def test_resamp_mean_month(self):
        
        pandas_df_test = pd.DataFrame({'Date': ['2022-06-01 6:00', '2022-06-01 6:01', '2022-06-01 6:02'
                                            , '2022-06-01 6:03', '2022-06-01 6:04', '2022-06-01 6:05'
                                            , '2022-06-01 6:06', '2022-06-01 6:07', '2022-06-01 6:08'
                                            , '2022-07-02 7:09', '2022-07-02 7:10', '2022-07-02 7:11'
                                            , '2022-08-03 8:12', '2022-08-03 8:13', '2022-08-03 8:14']
                          , 'tag_flow_1': [45, 56, 100, 8, 9, 63, 59, 56, 41, 8, 8, 53, 46, 44, 57]
                          , 'tag_flow_2': [51, 43, 59, 10, 11, 18, 93, 97, 45, 9, 10, 18, 27, 33, 38]
                                })

        pandas_df_test['Date'] = pd.to_datetime(pandas_df_test['Date'])
        pandas_df_test.set_index('Date',inplace=True)

        columns = ['tag_flow_1', 'tag_flow_2']
        
        pandas_df_actual = pd_resamp_mean(pandas_df_test, columns, 'M')

        pandas_df_expected = pd.DataFrame({'Date': ['2022-06-30', '2022-07-31', '2022-08-31']
                                  , 'tag_flow_1': [48.555556, 23.000000, 49.000000]
                                  , 'tag_flow_2': [47.444444, 12.333333, 32.666667]
                                        })

        pandas_df_expected['Date'] = pd.to_datetime(pandas_df_expected['Date'])
        pandas_df_expected.set_index('Date',inplace=True)
        pd.testing.assert_frame_equal(pandas_df_actual, pandas_df_expected, check_freq=False)
        
    def test_resamp_mean_year(self):
        
        pandas_df_test = pd.DataFrame({'Date': ['2020-06-01 6:00', '2020-06-01 6:01', '2020-06-01 6:02'
                                            , '2020-06-01 6:03', '2020-06-01 6:04', '2020-06-01 6:05'
                                            , '2020-06-01 6:06', '2020-06-01 6:07', '2020-06-01 6:08'
                                            , '2021-07-02 7:09', '2021-07-02 7:10', '2021-07-02 7:11'
                                            , '2022-08-03 8:12', '2022-08-03 8:13', '2022-08-03 8:14']
                          , 'tag_flow_1': [45, 56, 100, 8, 9, 63, 59, 56, 41, 8, 8, 53, 46, 44, 57]
                          , 'tag_flow_2': [51, 43, 59, 10, 11, 18, 93, 97, 45, 9, 10, 18, 27, 33, 38]
                                })

        pandas_df_test['Date'] = pd.to_datetime(pandas_df_test['Date'])
        pandas_df_test.set_index('Date',inplace=True)

        columns = ['tag_flow_1', 'tag_flow_2']
        
        pandas_df_actual = pd_resamp_mean(pandas_df_test, columns, 'Y')

        pandas_df_expected = pd.DataFrame({'Date': ['2020-12-31', '2021-12-31', '2022-12-31']
                                  , 'tag_flow_1': [48.555556, 23.000000, 49.000000]
                                  , 'tag_flow_2': [47.444444, 12.333333, 32.666667]
                                        })

        pandas_df_expected['Date'] = pd.to_datetime(pandas_df_expected['Date'])
        pandas_df_expected.set_index('Date',inplace=True)
        pd.testing.assert_frame_equal(pandas_df_actual, pandas_df_expected, check_freq=False)
        

class Pd_Resamp_Median_Test(unittest.TestCase):
    
    def test_resamp_median_hour(self):
        
        pandas_df_test = pd.DataFrame({'Date': ['2022-06-01 6:00', '2022-06-01 6:01', '2022-06-01 6:02'
                                            , '2022-06-01 6:03', '2022-06-01 6:04', '2022-06-01 6:05'
                                            , '2022-06-01 6:06', '2022-06-01 6:07', '2022-06-01 6:08'
                                            , '2022-06-01 7:09', '2022-06-01 7:10', '2022-06-01 7:11'
                                            , '2022-06-01 8:12', '2022-06-01 8:13']
                          , 'tag_flow_1': [45, 56, 100, 8, 9, 63, 59, 56, 41, 8, 8, 53, 46, 44]
                          , 'tag_flow_2': [51, 43, 59, 10, 11, 18, 93, 97, 45, 9, 10, 18, 27, 33]
                                })

        pandas_df_test['Date'] = pd.to_datetime(pandas_df_test['Date'])
        pandas_df_test.set_index('Date',inplace=True)

        columns = ['tag_flow_1', 'tag_flow_2']
        
        pandas_df_actual = pd_resamp_median(pandas_df_test, columns, 'H')
        

        pandas_df_expected = pd.DataFrame({'Date': ['2022-06-01 6:00', '2022-06-01 7:00', '2022-06-01 8:00']
                                  , 'tag_flow_1': [56.0, 8.0, 45.0]
                                  , 'tag_flow_2': [45.0, 10.0, 30.0]
                                        })
        
        pandas_df_expected['Date'] = pd.to_datetime(pandas_df_expected['Date'])
        pandas_df_expected.set_index('Date',inplace=True)
        pd.testing.assert_frame_equal(pandas_df_actual, pandas_df_expected, check_freq=False)
        
    def test_resamp_median_day(self):
        
        pandas_df_test = pd.DataFrame({'Date': ['2022-06-01 6:00', '2022-06-01 6:01', '2022-06-01 6:02'
                                            , '2022-06-01 6:03', '2022-06-01 6:04', '2022-06-01 6:05'
                                            , '2022-06-01 6:06', '2022-06-01 6:07', '2022-06-01 6:08'
                                            , '2022-06-02 7:09', '2022-06-02 7:10', '2022-06-02 7:11'
                                            , '2022-06-03 8:12', '2022-06-03 8:13']
                          , 'tag_flow_1': [45, 56, 100, 8, 9, 63, 59, 56, 41, 8, 8, 53, 46, 44]
                          , 'tag_flow_2': [51, 43, 59, 10, 11, 18, 93, 97, 45, 9, 10, 18, 27, 33]
                                })

        pandas_df_test['Date'] = pd.to_datetime(pandas_df_test['Date'])
        pandas_df_test.set_index('Date',inplace=True)

        columns = ['tag_flow_1', 'tag_flow_2']
        
        pandas_df_actual = pd_resamp_median(pandas_df_test, columns, 'D')

        pandas_df_expected = pd.DataFrame({'Date': ['2022-06-01', '2022-06-02', '2022-06-03']
                                  , 'tag_flow_1': [56.0, 8.0, 45.0]
                                  , 'tag_flow_2': [45.0, 10.0, 30.0]
                                        })

        pandas_df_expected['Date'] = pd.to_datetime(pandas_df_expected['Date'])
        pandas_df_expected.set_index('Date',inplace=True)
        pd.testing.assert_frame_equal(pandas_df_actual, pandas_df_expected, check_freq=False)
        
    def test_resamp_median_month(self):
        
        pandas_df_test = pd.DataFrame({'Date': ['2022-06-01 6:00', '2022-06-01 6:01', '2022-06-01 6:02'
                                            , '2022-06-01 6:03', '2022-06-01 6:04', '2022-06-01 6:05'
                                            , '2022-06-01 6:06', '2022-06-01 6:07', '2022-06-01 6:08'
                                            , '2022-07-02 7:09', '2022-07-02 7:10', '2022-07-02 7:11'
                                            , '2022-08-03 8:12', '2022-08-03 8:13']
                          , 'tag_flow_1': [45, 56, 100, 8, 9, 63, 59, 56, 41, 8, 8, 53, 46, 44]
                          , 'tag_flow_2': [51, 43, 59, 10, 11, 18, 93, 97, 45, 9, 10, 18, 27, 33]
                                })

        pandas_df_test['Date'] = pd.to_datetime(pandas_df_test['Date'])
        pandas_df_test.set_index('Date',inplace=True)

        columns = ['tag_flow_1', 'tag_flow_2']
        
        pandas_df_actual = pd_resamp_median(pandas_df_test, columns, 'M')

        pandas_df_expected = pd.DataFrame({'Date': ['2022-06-30', '2022-07-31', '2022-08-31']
                                  , 'tag_flow_1': [56.0, 8.0, 45.0]
                                  , 'tag_flow_2': [45.0, 10.0, 30.0]
                                        })

        pandas_df_expected['Date'] = pd.to_datetime(pandas_df_expected['Date'])
        pandas_df_expected.set_index('Date',inplace=True)
        pd.testing.assert_frame_equal(pandas_df_actual, pandas_df_expected, check_freq=False)
        
    def test_resamp_median_year(self):
        
        pandas_df_test = pd.DataFrame({'Date': ['2020-06-01 6:00', '2020-06-01 6:01', '2020-06-01 6:02'
                                            , '2020-06-01 6:03', '2020-06-01 6:04', '2020-06-01 6:05'
                                            , '2020-06-01 6:06', '2020-06-01 6:07', '2020-06-01 6:08'
                                            , '2021-07-02 7:09', '2021-07-02 7:10', '2021-07-02 7:11'
                                            , '2022-08-03 8:12', '2022-08-03 8:13']
                          , 'tag_flow_1': [45, 56, 100, 8, 9, 63, 59, 56, 41, 8, 8, 53, 46, 44]
                          , 'tag_flow_2': [51, 43, 59, 10, 11, 18, 93, 97, 45, 9, 10, 18, 27, 33]
                                })

        pandas_df_test['Date'] = pd.to_datetime(pandas_df_test['Date'])
        pandas_df_test.set_index('Date',inplace=True)

        columns = ['tag_flow_1', 'tag_flow_2']
        
        pandas_df_actual = pd_resamp_median(pandas_df_test, columns, 'Y')

        pandas_df_expected = pd.DataFrame({'Date': ['2020-12-31', '2021-12-31', '2022-12-31']
                                  , 'tag_flow_1': [56.0, 8.0, 45.0]
                                  , 'tag_flow_2': [45.0, 10.0, 30.0]
                                        })

        pandas_df_expected['Date'] = pd.to_datetime(pandas_df_expected['Date'])
        pandas_df_expected.set_index('Date',inplace=True)
        pd.testing.assert_frame_equal(pandas_df_actual, pandas_df_expected, check_freq=False)
        
class Pd_Resamp_Last_Test(unittest.TestCase):
    
    def test_resamp_last_hour(self):
        
        pandas_df_test = pd.DataFrame({'Date': ['2022-06-01 6:00', '2022-06-01 6:01', '2022-06-01 6:02'
                                            , '2022-06-01 6:03', '2022-06-01 6:04', '2022-06-01 6:05'
                                            , '2022-06-01 6:06', '2022-06-01 6:07', '2022-06-01 6:08'
                                            , '2022-06-01 7:09', '2022-06-01 7:10', '2022-06-01 7:11'
                                            , '2022-06-01 8:12', '2022-06-01 8:13']
                          , 'tag_flow_1': [45, 56, 100, 8, 9, 63, 59, 56, 41, 8, 8, 53, 46, 44]
                          , 'tag_flow_2': [51, 43, 59, 10, 11, 18, 93, 97, 45, 9, 10, 18, 27, 33]
                                })

        pandas_df_test['Date'] = pd.to_datetime(pandas_df_test['Date'])
        pandas_df_test.set_index('Date',inplace=True)

        columns = ['tag_flow_1', 'tag_flow_2']
        
        pandas_df_actual = pd_resamp_last(pandas_df_test, columns, 'H')
        

        pandas_df_expected = pd.DataFrame({'Date': ['2022-06-01 6:00', '2022-06-01 7:00', '2022-06-01 8:00']
                                  , 'tag_flow_1': [41, 53, 44]
                                  , 'tag_flow_2': [45, 18, 33]
                                        })
        
        pandas_df_expected['Date'] = pd.to_datetime(pandas_df_expected['Date'])
        pandas_df_expected.set_index('Date',inplace=True)
        pd.testing.assert_frame_equal(pandas_df_actual, pandas_df_expected, check_freq=False)
        
    def test_resamp_last_day(self):
        
        pandas_df_test = pd.DataFrame({'Date': ['2022-06-01 6:00', '2022-06-01 6:01', '2022-06-01 6:02'
                                            , '2022-06-01 6:03', '2022-06-01 6:04', '2022-06-01 6:05'
                                            , '2022-06-01 6:06', '2022-06-01 6:07', '2022-06-01 6:08'
                                            , '2022-06-02 7:09', '2022-06-02 7:10', '2022-06-02 7:11'
                                            , '2022-06-03 8:12', '2022-06-03 8:13']
                          , 'tag_flow_1': [45, 56, 100, 8, 9, 63, 59, 56, 41, 8, 8, 53, 46, 44]
                          , 'tag_flow_2': [51, 43, 59, 10, 11, 18, 93, 97, 45, 9, 10, 18, 27, 33]
                                })

        pandas_df_test['Date'] = pd.to_datetime(pandas_df_test['Date'])
        pandas_df_test.set_index('Date',inplace=True)

        columns = ['tag_flow_1', 'tag_flow_2']
        
        pandas_df_actual = pd_resamp_last(pandas_df_test, columns, 'D')

        pandas_df_expected = pd.DataFrame({'Date': ['2022-06-01', '2022-06-02', '2022-06-03']
                                  , 'tag_flow_1': [41, 53, 44]
                                  , 'tag_flow_2': [45, 18, 33]
                                        })

        pandas_df_expected['Date'] = pd.to_datetime(pandas_df_expected['Date'])
        pandas_df_expected.set_index('Date',inplace=True)
        pd.testing.assert_frame_equal(pandas_df_actual, pandas_df_expected, check_freq=False)
        
    def test_resamp_last_month(self):
        
        pandas_df_test = pd.DataFrame({'Date': ['2022-06-01 6:00', '2022-06-01 6:01', '2022-06-01 6:02'
                                            , '2022-06-01 6:03', '2022-06-01 6:04', '2022-06-01 6:05'
                                            , '2022-06-01 6:06', '2022-06-01 6:07', '2022-06-01 6:08'
                                            , '2022-07-02 7:09', '2022-07-02 7:10', '2022-07-02 7:11'
                                            , '2022-08-03 8:12', '2022-08-03 8:13']
                          , 'tag_flow_1': [45, 56, 100, 8, 9, 63, 59, 56, 41, 8, 8, 53, 46, 44]
                          , 'tag_flow_2': [51, 43, 59, 10, 11, 18, 93, 97, 45, 9, 10, 18, 27, 33]
                                })

        pandas_df_test['Date'] = pd.to_datetime(pandas_df_test['Date'])
        pandas_df_test.set_index('Date',inplace=True)

        columns = ['tag_flow_1', 'tag_flow_2']
        
        pandas_df_actual = pd_resamp_last(pandas_df_test, columns, 'M')

        pandas_df_expected = pd.DataFrame({'Date': ['2022-06-30', '2022-07-31', '2022-08-31']
                                  , 'tag_flow_1': [41, 53, 44]
                                  , 'tag_flow_2': [45, 18, 33]
                                        })

        pandas_df_expected['Date'] = pd.to_datetime(pandas_df_expected['Date'])
        pandas_df_expected.set_index('Date',inplace=True)
        pd.testing.assert_frame_equal(pandas_df_actual, pandas_df_expected, check_freq=False)
        
    def test_resamp_last_year(self):
        
        pandas_df_test = pd.DataFrame({'Date': ['2020-06-01 6:00', '2020-06-01 6:01', '2020-06-01 6:02'
                                            , '2020-06-01 6:03', '2020-06-01 6:04', '2020-06-01 6:05'
                                            , '2020-06-01 6:06', '2020-06-01 6:07', '2020-06-01 6:08'
                                            , '2021-07-02 7:09', '2021-07-02 7:10', '2021-07-02 7:11'
                                            , '2022-08-03 8:12', '2022-08-03 8:13']
                          , 'tag_flow_1': [45, 56, 100, 8, 9, 63, 59, 56, 41, 8, 8, 53, 46, 44]
                          , 'tag_flow_2': [51, 43, 59, 10, 11, 18, 93, 97, 45, 9, 10, 18, 27, 33]
                                })

        pandas_df_test['Date'] = pd.to_datetime(pandas_df_test['Date'])
        pandas_df_test.set_index('Date',inplace=True)

        columns = ['tag_flow_1', 'tag_flow_2']
        
        pandas_df_actual = pd_resamp_last(pandas_df_test, columns, 'Y')

        pandas_df_expected = pd.DataFrame({'Date': ['2020-12-31', '2021-12-31', '2022-12-31']
                                  , 'tag_flow_1': [41, 53, 44]
                                  , 'tag_flow_2': [45, 18, 33]
                                        })

        pandas_df_expected['Date'] = pd.to_datetime(pandas_df_expected['Date'])
        pandas_df_expected.set_index('Date',inplace=True)
        pd.testing.assert_frame_equal(pandas_df_actual, pandas_df_expected, check_freq=False)
