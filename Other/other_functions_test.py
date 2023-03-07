# Databricks notebook source
import unittest

class TestGetShiftidFromTimestamp(unittest.TestCase):

    def test_get_shiftid_from_timestamp_day_shift(self):
        timestamp = datetime(2023, 3, 3, 8, 0, 0)
        day_shift_start_time = time(8, 0, 0)
        expected_shiftid = 230303001
        shiftid = get_shiftid_from_timestamp(timestamp, day_shift_start_time)
        self.assertEqual(shiftid, expected_shiftid)

    def test_get_shiftid_from_timestamp_night_shift(self):
        timestamp = datetime(2023, 3, 3, 22, 30, 0)
        day_shift_start_time = time(5, 30, 0)
        expected_shiftid = 230303002
        shiftid = get_shiftid_from_timestamp(timestamp, day_shift_start_time)
        self.assertEqual(shiftid, expected_shiftid)

    def test_get_shiftid_from_timestamp_past_midnight(self):
        timestamp = datetime(2023, 3, 4, 2, 30, 0)
        day_shift_start_time = time(5, 30, 0)
        expected_shiftid = 230303002
        shiftid = get_shiftid_from_timestamp(timestamp, day_shift_start_time)
        self.assertEqual(shiftid, expected_shiftid)

    def test_get_shiftid_from_timestamp_at_midnight(self):
        timestamp = datetime(2023, 3, 3, 0, 0, 0)
        day_shift_start_time = time(8, 0, 0)
        expected_shiftid = 230302002
        shiftid = get_shiftid_from_timestamp(timestamp, day_shift_start_time)
        self.assertEqual(shiftid, expected_shiftid)

    def test_get_shiftid_from_timestamp_at_end_of_day_shift(self):
        timestamp = datetime(2023, 3, 3, 19, 59, 59, 999999)
        day_shift_start_time = time(8, 0, 0)
        expected_shiftid = 230303001
        shiftid = get_shiftid_from_timestamp(timestamp, day_shift_start_time)
        self.assertEqual(shiftid, expected_shiftid)

    def test_timestamp_at_start_of_night_shift(self):
        timestamp = datetime(2023, 3, 3, 17, 30, 0)
        day_shift_start_time = time(17, 30)
        expected_shiftid = 230303002
        shiftid = get_shiftid_from_timestamp(timestamp, day_shift_start_time)
        self.assertEqual(shiftid, expected_shiftid)

class GetDateRangeTests(unittest.TestCase):
    
    def test_get_date_range_multiple_dates(self):
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 3)
        actual = get_date_range(start_date, end_date)
        expected = [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)]
        self.assertEqual(actual, expected)

    def test_get_date_range_one_date(self):
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 1)
        actual = get_date_range(start_date, end_date)
        expected = [date(2023, 1, 1)]
        self.assertEqual(actual, expected)

    def test_get_date_range_wrong_order(self):
        start_date = date(2023, 1, 3)
        end_date = date(2023, 1, 1)
        actual = get_date_range(start_date, end_date)
        expected = []
        self.assertEqual(actual, expected)

class GetNRowsByColumnTests(unittest.TestCase):

    def setUp(self):
        data = [{'first_name': 'Jane', 'last_name': 'Doe', 'age': 10}
              , {'first_name': 'Play', 'last_name': 'Doe', 'age': 20}
              , {'first_name': 'Taekwon', 'last_name': 'Doe', 'age': 35}
               ]
        self.spark_df_test = spark.createDataFrame(data)

    def test_get_n_rows_by_column_1_row_desc(self):
        actual_result = get_n_rows_by_column(self.spark_df_test, 'age', 1, sort_ascending = False).collect()
        expected_data = [{'first_name': 'Taekwon', 'last_name': 'Doe', 'age': 35}]
        expected_result = spark.createDataFrame(expected_data).collect()
        self.assertEqual(actual_result, expected_result)

    def test_get_n_rows_by_column_2_rows_asc(self):
        actual_result = get_n_rows_by_column(self.spark_df_test, 'age', 2, sort_ascending = True).collect()
        expected_data = [{'first_name': 'Jane', 'last_name': 'Doe', 'age': 10}
                       , {'first_name': 'Play', 'last_name': 'Doe', 'age': 20}
                        ]
        expected_result = spark.createDataFrame(expected_data).collect()
        self.assertEqual(actual_result, expected_result)

    def test_get_n_rows_by_column_all_rows_desc(self):
        actual_result = get_n_rows_by_column(self.spark_df_test, 'age', 20, sort_ascending = False).collect()
        expected_data = [{'first_name': 'Taekwon', 'last_name': 'Doe', 'age': 35}
                       , {'first_name': 'Play', 'last_name': 'Doe', 'age': 20}
                       , {'first_name': 'Jane', 'last_name': 'Doe', 'age': 10}
                        ]
        expected_result = spark.createDataFrame(expected_data).collect()
        self.assertEqual(actual_result, expected_result)

class AddRollingWindowAggregationColumnTests(unittest.TestCase):

    def test_add_rolling_window_aggregation_column_sum(self):
        spark_df_test = spark.createDataFrame([('2023-03-10 15:27:18', 17)
                                             , ('2023-03-15 12:27:18', 13)
                                             , ('2023-03-18 11:27:18', 25)
                                              ]
                                             , ['timestamp', 'dollars'])
        spark_df_test = spark_df_test.withColumn('timestamp', spark_df_test.timestamp.cast('timestamp'))
        
        config = {'aggregate_column_name': 'dollars'
               , 'aggregate_function_name': 'sum'
                 }
        spark_df_actual = add_rolling_window_aggregation_column(spark_df_test, config)

        spark_df_expected = spark.createDataFrame([('2023-03-10 15:27:18', 17, 17.0)
                                                 , ('2023-03-15 12:27:18', 13, 30.0)
                                                 , ('2023-03-18 11:27:18', 25, 55.0)
                                                  ]
                                                 , ['timestamp', 'dollars', 'dollars_rolling_sum'])
        spark_df_expected = spark_df_expected.withColumn('timestamp', spark_df_expected.timestamp.cast('timestamp'))                                                  

        self.assertEqual(spark_df_actual.collect(), spark_df_expected.collect())

    def test_add_rolling_window_aggregation_column_avg(self):
        spark_df_test = spark.createDataFrame([('2023-03-10 15:27:18', 17)
                                             , ('2023-03-15 12:27:18', 13)
                                             , ('2023-03-18 11:27:18', 25)
                                             , ('2023-03-19 11:27:18', 30)
                                             , ('2023-03-20 11:27:18', 45)
                                              ]
                                             , ['timestamp', 'dollars'])
        spark_df_test = spark_df_test.withColumn('timestamp', spark_df_test.timestamp.cast('timestamp'))
        
        config = {'aggregate_column_name': 'dollars'}
        spark_df_actual = add_rolling_window_aggregation_column(spark_df_test, config)
        spark_df_actual = spark_df_actual.withColumn('dollars_rolling_avg', round('dollars_rolling_avg', 4))

        spark_df_expected = spark.createDataFrame([('2023-03-10 15:27:18', 17, 17.0)
                                                 , ('2023-03-15 12:27:18', 13, 15.0)
                                                 , ('2023-03-18 11:27:18', 25, 18.3333)
                                                 , ('2023-03-19 11:27:18', 30, 21.25)
                                                 , ('2023-03-20 11:27:18', 45, 26.0)
                                                  ]
                                                 , ['timestamp', 'dollars', 'dollars_rolling_avg'])
        spark_df_expected = spark_df_expected.withColumn('timestamp', spark_df_expected.timestamp.cast('timestamp'))                                                  

        self.assertEqual(spark_df_actual.collect(), spark_df_expected.collect())