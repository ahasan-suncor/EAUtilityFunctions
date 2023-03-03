# Databricks notebook source
import unittest

class TestLoadDataFromPath(unittest.TestCase):
    
    def setUp(self):
        self.data_path = '/tmp/TestLoadDataFromPath/TestData'
        data = [{'first_name': 'Jane', 'last_name': 'Doe', 'age': 10}
              , {'first_name': 'Play', 'last_name': 'Doe', 'age': 20}
              , {'first_name': 'Taekwon', 'last_name': 'Doe', 'age': 35}
               ]
        self.spark_df_test = spark_df_test = spark.createDataFrame(data)
        self.spark_df_test.write.format('csv').option('header', True).mode('overwrite').save(self.data_path)

    @classmethod
    def tearDown(cls):
        dbutils.fs.rm("/tmp/TestLoadDataFromPath/TestData", True)

    def test_load_data_from_path(self):
        spark_df_actual = load_data_from_path(self.data_path, data_format = 'csv')
        self.assertEqual(spark_df_actual.columns, ['_c0', '_c1', '_c2'])
        self.assertEqual(spark_df_actual.count(), 7)

    def test_load_data_from_path_with_options(self):
        data_options = {'header': True}
        spark_df_actual = load_data_from_path(self.data_path, data_format = 'csv', data_options = data_options)
        self.assertEqual(spark_df_actual.count(), 3)
        self.assertEqual(spark_df_actual.columns, ['age', 'first_name', 'last_name'])

    def test_load_data_from_path_no_path(self):
        spark_df_actual = load_data_from_path(None)
        self.assertEqual(spark_df_actual, None)

class TestAddColumnPrefix(unittest.TestCase):
    
    def setUp(self):
        self.spark_df_test = spark.createDataFrame([('Jane', 'Doe', 10), ('Play', 'Doe', 20), ('Taekwon', 'Doe', 35)]
                                                 , ['first_name', 'last_name', 'age'])

    def test_add_column_prefix_with_prefix(self):
        prefixed_df = add_column_prefix(self.spark_df_test, 'human_')
        self.assertEqual(prefixed_df.columns, ['human_first_name', 'human_last_name', 'human_age'])

    def test_add_column_prefix_empty_prefix(self):
        prefixed_df = add_column_prefix(self.spark_df_test, '')
        self.assertEqual(prefixed_df.columns, ['first_name', 'last_name', 'age'])

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

