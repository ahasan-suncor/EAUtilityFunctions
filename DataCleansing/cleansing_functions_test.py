# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType
import unittest

class NormalizeColumnsTests(unittest.TestCase):
    def test_normalize_column_names_with_data(self):
        data = [{'Product Category': 'A', 'ID': 1, 'Value': 121.44, 'some_column_': True}
              , {'Product Category': 'B', 'ID': 2, 'Value': 300.01, 'some_column_': False}
              , {'Product Category': 'C', 'ID': 3, 'Value': 10.99, 'some_column_': None}
              , {'Product Category': 'E', 'ID': 4, 'Value': 33.87, 'some_column_': True}
               ]
        spark_df_test = spark.createDataFrame(data)
        normalized_df = normalize_column_names(spark_df_test)
        actual_columns = normalized_df.columns
        expected_columns = ['product_category', 'id', 'value', 'some_column_']
        self.assertCountEqual(expected_columns, actual_columns)
    
    def test_normalize_column_names_with_empty_df(self):
        empty_schema = StructType([StructField("column one", StringType(), True)
                                 , StructField("COLUMnTwO", StringType(), True)
                                 , StructField("column three", StringType(), True)
                                 ])
        spark_df_empty = spark.createDataFrame([], schema = empty_schema)
        normalized_df = normalize_column_names(spark_df_empty)
        actual_columns = normalized_df.columns
        expected_columns = ['column_one', 'columntwo', 'column_three']
        self.assertCountEqual(expected_columns, actual_columns)
    
    def test_normalize_column_names_with_special_characters(self):
        data = [{'col_on$e': 123
               , 'col @ 2': 1
               , 'col!_4': 121.44
               , '': 1
               , '(fine_column)': True}
               ]
        spark_df_test = spark.createDataFrame(data)
        normalized_df = normalize_column_names(spark_df_test)
        actual_columns = normalized_df.columns
        expected_columns = ['', 'col_@_2', 'col!_4', 'col_on$e', '(fine_column)']
        self.assertCountEqual(expected_columns, actual_columns)
        
class RenameColumnsAfterAggTests(unittest.TestCase):
    def test_rename_columns_after_agg_with_data(self):
        data = [{'timestamp': '2023-02-23 23:23:02', 'first(tag_1)': 12.3, 'max(tag_2)': 121.44}
              , {'timestamp': '2023-02-23 23:23:12', 'first(tag_1)': 1.3, 'max(tag_2)': 421.44}
               ]
        spark_df_test = spark.createDataFrame(data)
        normalized_df = rename_columns_after_agg(spark_df_test)
        actual_columns = normalized_df.columns
        expected_columns = expected_columns = ['timestamp', 'tag_1', 'tag_2']
        self.assertCountEqual(expected_columns, actual_columns)
    
    def test_rename_columns_after_agg_with_single_column(self):
        data = [{'max(tag_2)': 121.44}]
        spark_df_test = spark.createDataFrame(data)
        normalized_df = rename_columns_after_agg(spark_df_test)
        actual_columns = normalized_df.columns
        expected_columns = expected_columns = ['tag_2']
        self.assertCountEqual(expected_columns, actual_columns)
        
    def test_rename_columns_after_agg_with_empty_df(self):
        empty_schema = StructType([StructField('column one', StringType(), True)
                                 , StructField('min(COLUMnTwO)', StringType(), True)
                                 , StructField('(column three)', StringType(), True)
                                 , StructField('', StringType(), True)
                                 ])
        spark_df_empty = spark.createDataFrame([], schema = empty_schema)
        normalized_df = rename_columns_after_agg(spark_df_empty)
        actual_columns = normalized_df.columns
        expected_columns = expected_columns = ['column one', 'COLUMnTwO', 'column three', '']
        self.assertCountEqual(expected_columns, actual_columns)
        
class FilterPriorityProcessDataTests(unittest.TestCase):
    def test_filter_priority_process_data_with_multiple_tags(self):
        data = [{'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_1', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:12', 'tag_name': 'tag_1', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_2', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_3', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:22', 'tag_name': 'tag_3', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:32', 'tag_name': 'tag_3', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_4', 'value': 121.44}
               ]
        spark_df_test = spark.createDataFrame(data)
        priority_tags = ['tag_4', 'tag_2']
        priority_tags_dict = {'tag_column_name': 'tag_name', 'priority_tags': priority_tags}
        spark_df_filtered = filter_priority_process_data(spark_df_test, priority_tags_dict)
        actual_tags_in_col = spark_df_filtered.select('tag_name').distinct()
        expected_tags_in_col = priority_tags
        self.assertTrue(expected_tags_in_col == expected_tags_in_col)
        
    def test_filter_priority_process_data_with_no_tags(self):
        data = [{'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_1', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:12', 'tag_name': 'tag_1', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_2', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_3', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:22', 'tag_name': 'tag_3', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:32', 'tag_name': 'tag_3', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_4', 'value': 121.44}
               ]
        spark_df_test = spark.createDataFrame(data)
        priority_tags = []
        priority_tags_dict = {'tag_column_name': 'tag_name', 'priority_tags': priority_tags}
        spark_df_filtered = filter_priority_process_data(spark_df_test, priority_tags_dict)
        actual_tags_in_col = spark_df_filtered.select('tag_name').distinct()
        expected_tags_in_col = priority_tags
        self.assertTrue(expected_tags_in_col == expected_tags_in_col)
        
    def test_filter_priority_process_data_with_no_tags_filtered(self):
        data = [{'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_1', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:12', 'tag_name': 'tag_1', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_2', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_3', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:22', 'tag_name': 'tag_3', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:32', 'tag_name': 'tag_3', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_4', 'value': 121.44}
               ]
        spark_df_test = spark.createDataFrame(data)
        priority_tags = ['tag_1', 'tag_2', 'tag_3', 'tag_4']
        priority_tags_dict = {'tag_column_name': 'tag_name', 'priority_tags': priority_tags}
        spark_df_filtered = filter_priority_process_data(spark_df_test, priority_tags_dict)
        actual_tags_in_col = spark_df_filtered.select('tag_name').distinct()
        expected_tags_in_col = priority_tags
        self.assertTrue(expected_tags_in_col == expected_tags_in_col)
        
    def test_filter_priority_process_data_with_some_tags_filtered(self):
        data = [{'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_1', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:12', 'tag_name': 'tag_1', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_2', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_3', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:22', 'tag_name': 'tag_3', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:32', 'tag_name': 'tag_3', 'value': 121.44}
               ]
        spark_df_test = spark.createDataFrame(data)
        priority_tags = ['tag_1', 'tag_2', 'tag_3', 'tag_4']
        priority_tags_dict = {'tag_column_name': 'tag_name', 'priority_tags': priority_tags}
        spark_df_filtered = filter_priority_process_data(spark_df_test, priority_tags_dict)
        actual_tags_in_col = spark_df_filtered.select('tag_name').distinct()
        expected_tags_in_col = ['tag_1', 'tag_2', 'tag_3']
        self.assertTrue(expected_tags_in_col == expected_tags_in_col)
        
    def test_filter_priority_process_data_with_no_data(self):
        empty_schema = StructType([StructField('timestamp', StringType(), True)
                                 , StructField('tag_name', StringType(), True)
                                 , StructField('value', StringType(), True)
                                 ])
        spark_df_test = spark.createDataFrame([], schema = empty_schema)
        priority_tags = ['tag_1', 'tag_2', 'tag_3', 'tag_4']
        priority_tags_dict = {'tag_column_name': 'tag_name', 'priority_tags': priority_tags}
        spark_df_filtered = filter_priority_process_data(spark_df_test, priority_tags_dict)
        actual_tags_in_col = spark_df_filtered.select('tag_name').distinct()
        expected_tags_in_col = []
        self.assertTrue(expected_tags_in_col == expected_tags_in_col)
        
class TypeCastTagDataTests(unittest.TestCase):
    def test_type_cast_tag_data_double(self):
        actual_data = [{'timestamp': '2023-02-23 23:23:02', 'tag_1': 121.44, 'tag_2': 32.9}
                     , {'timestamp': '2023-02-23 23:23:02', 'tag_1': 120.1, 'tag_2': 89.2}
                     , {'timestamp': '2023-02-23 23:23:02', 'tag_1': 11.44, 'tag_2': 23.8}
                      ]
        spark_df_test = spark.createDataFrame(actual_data)
        tags_data_type_dict = {'tag_1': 'double'
                             , 'tag_2': 'double'
                               }
        spark_df_actual = type_cast_tag_data(spark_df_test, tags_data_type_dict)
        
        expected_schema = {'timestamp': 'string', 'tag_1': 'double', 'tag_2': 'double'}
                
        for field in expected_schema:
            self.assertEqual(spark_df_actual.schema[field].dataType.simpleString(), expected_schema[field])
            
    def test_type_cast_tag_data_string(self):
        actual_data = [{'timestamp': '2023-02-23 23:23:02', 'tag_1': '121.44', 'tag_2': '32.9'}
                     , {'timestamp': '2023-02-23 23:23:02', 'tag_1': '120.1', 'tag_2': '89.2'}
                     , {'timestamp': '2023-02-23 23:23:02', 'tag_1': '11.44', 'tag_2': '23.8'}
                      ]
        spark_df_test = spark.createDataFrame(actual_data)
        tags_data_type_dict = {'tag_1': 'double'
                             , 'tag_2': 'double'
                               }
        spark_df_actual = type_cast_tag_data(spark_df_test, tags_data_type_dict)
        
        expected_schema = {'timestamp': 'string', 'tag_1': 'double', 'tag_2': 'double'}
                
        for field in expected_schema:
            self.assertEqual(spark_df_actual.schema[field].dataType.simpleString(), expected_schema[field])
            
    def test_type_cast_tag_data_mixed(self):
        actual_data = [{'timestamp': '2023-02-23 23:23:02', 'tag_1': '121.44', 'tag_2': 'Calc Failed'}
                     , {'timestamp': '2023-02-23 23:23:02', 'tag_1': 'BAD', 'tag_2': '89.2'}
                     , {'timestamp': '2023-02-23 23:23:02', 'tag_1': '11.44', 'tag_2': '23.8'}
                      ]
        spark_df_test = spark.createDataFrame(actual_data)
        tags_data_type_dict = {'tag_1': 'double'
                             , 'tag_2': 'double'
                               }
        spark_df_actual = type_cast_tag_data(spark_df_test, tags_data_type_dict)
        
        expected_schema = {'timestamp': 'string', 'tag_1': 'double', 'tag_2': 'double'}
                
        for field in expected_schema:
            self.assertEqual(spark_df_actual.schema[field].dataType.simpleString(), expected_schema[field])
            
    def test_type_cast_tag_data_with_invalid_types(self):
        actual_data = [{'timestamp': '2023-02-23 23:23:02', 'tag_1': 121.44}
                     , {'timestamp': '2023-02-23 23:23:02', 'tag_1': 120.1}
                     , {'timestamp': '2023-02-23 23:23:02', 'tag_1': 11.44}
                      ]
        spark_df_test = spark.createDataFrame(actual_data)
        tags_data_type_dict = {'tag_1': 'integer'}
        spark_df_actual = type_cast_tag_data(spark_df_test, tags_data_type_dict)
        
        with self.assertRaises(Exception):
            type_cast_tag_data(self.spark_df_test, tags_data_type_dict)
        
class PivotProcessDataTests(unittest.TestCase):
    def test_pivot_process_data_multiple_tags(self):
        data = [{'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_1', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:12', 'tag_name': 'tag_1', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_2', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_3', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:22', 'tag_name': 'tag_3', 'value': 121.44}
              , {'timestamp': '2023-02-23 23:23:32', 'tag_name': 'tag_3', 'value': 121.44}
               ]
        spark_df_test = spark.createDataFrame(data)
        spark_df_actual = pivot_process_data(spark_df_test, pivot_column_name = 'tag_name', aggregate_column_name = 'value')
        
        expected_data = [{'timestamp': '2023-02-23 23:23:02', 'tag_1': 121.44, 'tag_2': 121.44, 'tag_3': 121.44}
                       , {'timestamp': '2023-02-23 23:23:32', 'tag_1': None, 'tag_2': None, 'tag_3': 121.44}
                       , {'timestamp': '2023-02-23 23:23:12', 'tag_1': 121.44, 'tag_2': None, 'tag_3': None}
                       , {'timestamp': '2023-02-23 23:23:22', 'tag_1': None, 'tag_2': None, 'tag_3': 121.44}
                        ]
        spark_df_expected = spark.createDataFrame(expected_data)
        
        # Select the columns in the same order, and sort both dataframes before comparing them.
        spark_df_actual_sorted = spark_df_actual.select(['timestamp', 'tag_1', 'tag_2', 'tag_3']).orderBy('timestamp')
        spark_df_expected_sorted = spark_df_expected.select(['timestamp', 'tag_1', 'tag_2', 'tag_3']).orderBy('timestamp')

        self.assertTrue(is_actual_df_equal_to_expected_df(spark_df_actual_sorted, spark_df_expected_sorted) == True)
        
    def test_pivot_process_data_no_aggregation(self):
        data = [{'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_1', 'value': 121.44}
              , {'timestamp': '2023-02-21 23:23:12', 'tag_name': 'tag_1', 'value': 121.44}
              , {'timestamp': '2023-02-24 23:23:02', 'tag_name': 'tag_2', 'value': 121.44}
              , {'timestamp': '2023-02-26 23:23:22', 'tag_name': 'tag_2', 'value': 121.44}
              , {'timestamp': '2023-02-27 23:23:32', 'tag_name': 'tag_3', 'value': 121.44}
               ]
        spark_df_test = spark.createDataFrame(data)
        spark_df_actual = pivot_process_data(spark_df_test, pivot_column_name = 'tag_name', aggregate_column_name = 'value')
        
        expected_data = [{'timestamp': '2023-02-23 23:23:02', 'tag_1': 121.44, 'tag_2': None, 'tag_3': None}
                       , {'timestamp': '2023-02-21 23:23:12', 'tag_1': 121.44, 'tag_2': None, 'tag_3': None}
                       , {'timestamp': '2023-02-24 23:23:02', 'tag_1': None, 'tag_2': 121.44, 'tag_3': None}
                       , {'timestamp': '2023-02-26 23:23:22', 'tag_1': None, 'tag_2': 121.44, 'tag_3': None}
                       , {'timestamp': '2023-02-27 23:23:32', 'tag_1': None, 'tag_2': None, 'tag_3': 121.44}
                        ]
        spark_df_expected = spark.createDataFrame(expected_data)
        
        # Select the columns in the same order, and sort both dataframes before comparing them.
        spark_df_actual_sorted = spark_df_actual.select(['timestamp', 'tag_1', 'tag_2', 'tag_3']).orderBy('timestamp')
        spark_df_expected_sorted = spark_df_expected.select(['timestamp', 'tag_1', 'tag_2', 'tag_3']).orderBy('timestamp')

        self.assertTrue(is_actual_df_equal_to_expected_df(spark_df_actual_sorted, spark_df_expected_sorted) == True)
        
    def test_pivot_process_data_one_tag(self):
        data = [{'timestamp': '2023-02-23 23:23:02', 'tag_name': 'tag_1', 'value': 12.44}
              , {'timestamp': '2023-02-21 23:23:12', 'tag_name': 'tag_1', 'value': 34.44}
              , {'timestamp': '2023-02-24 23:23:02', 'tag_name': 'tag_1', 'value': 34.44}
               ]
        spark_df_test = spark.createDataFrame(data)
        spark_df_actual = pivot_process_data(spark_df_test, pivot_column_name = 'tag_name', aggregate_column_name = 'value')
        
        expected_data = [{'timestamp': '2023-02-23 23:23:02', 'tag_1': 12.44}
                       , {'timestamp': '2023-02-21 23:23:12', 'tag_1': 34.44}
                       , {'timestamp': '2023-02-24 23:23:02', 'tag_1': 34.44}
                        ]
        spark_df_expected = spark.createDataFrame(expected_data)
        
        # Select the columns in the same order, and sort both dataframes before comparing them.
        spark_df_actual_sorted = spark_df_actual.select(['timestamp', 'tag_1']).orderBy('timestamp')
        spark_df_expected_sorted = spark_df_expected.select(['timestamp', 'tag_1']).orderBy('timestamp')

        self.assertTrue(is_actual_df_equal_to_expected_df(spark_df_actual_sorted, spark_df_expected_sorted) == True)

class CleanProcessDataWithOutliersTests(unittest.TestCase):
    def test_clean_process_data_with_outliers_with_data(self):
        data = [{'timestamp': '2023-02-23 23:23:00', 'tag_1': 12.3, 'tag_2': 121.44}
              , {'timestamp': '2023-02-23 23:24:00', 'tag_1': 1.3, 'tag_2': 421.44}
              , {'timestamp': '2023-02-23 23:25:00', 'tag_1': 2.3, 'tag_2': 321.44}
              , {'timestamp': '2023-02-23 23:26:00', 'tag_1': 100.3, 'tag_2': 21.44}
              , {'timestamp': '2023-02-24 23:26:00', 'tag_1': 110.0, 'tag_2': 21.44}
              , {'timestamp': '2023-02-24 23:26:00', 'tag_1': 1100.3, 'tag_2': 300.44}
               ]
        spark_df_test = spark.createDataFrame(data)
        outliers_info_dict = {'tag_1': [10, 110], 'tag_2': [300, 350]}
        spark_df_actual = clean_process_data_with_outliers(spark_df_test, outliers_info_dict)
        
        expected_data = [{'timestamp': '2023-02-23 23:23:00', 'tag_1': 12.3, 'tag_2': None}
                       , {'timestamp': '2023-02-23 23:24:00', 'tag_1': None, 'tag_2': None}
                       , {'timestamp': '2023-02-23 23:25:00', 'tag_1': None, 'tag_2': 321.44}
                       , {'timestamp': '2023-02-23 23:26:00', 'tag_1': 100.3, 'tag_2': None}
                       , {'timestamp': '2023-02-24 23:26:00', 'tag_1': 110.0, 'tag_2': None}
                       , {'timestamp': '2023-02-24 23:26:00', 'tag_1': None, 'tag_2': 300.44}
                        ]
        spark_df_expected = spark.createDataFrame(expected_data)
        
        # Select the columns in the same order, and sort both dataframes before comparing them.
        spark_df_actual_sorted = spark_df_actual.select(['timestamp', 'tag_1', 'tag_2']).orderBy('timestamp')
        spark_df_expected_sorted = spark_df_expected.select(['timestamp', 'tag_1', 'tag_2']).orderBy('timestamp')

        self.assertTrue(is_actual_df_equal_to_expected_df(spark_df_actual_sorted, spark_df_expected_sorted) == True)
        
    def test_clean_process_data_with_outliers_with_no_data(self):
        empty_schema = StructType([StructField('timestamp', StringType(), True)
                                 , StructField('tag_1', StringType(), True)
                                 , StructField('tag_2', StringType(), True)
                                 , StructField('tag_3', StringType(), True)
                                 ])
        spark_df_empty = spark.createDataFrame([], schema = empty_schema)
        outliers_info_dict = {'tag_1': [10, 110], 'tag_2': [300, 350]}
        spark_df_actual = clean_process_data_with_outliers(spark_df_empty, outliers_info_dict)
        
        spark_df_expected = spark.createDataFrame([], schema = empty_schema)
        
        # Select the columns in the same order, and sort both dataframes before comparing them.
        spark_df_actual_sorted = spark_df_actual.select(['timestamp', 'tag_1', 'tag_2', 'tag_3']).orderBy('timestamp')
        spark_df_expected_sorted = spark_df_expected.select(['timestamp', 'tag_1', 'tag_2', 'tag_3']).orderBy('timestamp')

        self.assertTrue(is_actual_df_equal_to_expected_df(spark_df_actual_sorted, spark_df_expected_sorted) == True)
        
    def test_clean_process_data_with_outliers_with_data_one_tag_filter(self):
        data = [{'timestamp': '2023-02-23 23:23:00', 'tag_1': 12.3, 'tag_2': 121.44}
              , {'timestamp': '2023-02-23 23:24:00', 'tag_1': 1.3, 'tag_2': 421.44}
              , {'timestamp': '2023-02-23 23:25:00', 'tag_1': 2.3, 'tag_2': 321.44}
              , {'timestamp': '2023-02-23 23:26:00', 'tag_1': 100.3, 'tag_2': 21.44}
               ]
        spark_df_test = spark.createDataFrame(data)
        outliers_info_dict = {'tag_1': [10, 110]}
        spark_df_actual = clean_process_data_with_outliers(spark_df_test, outliers_info_dict)
        
        expected_data = [{'timestamp': '2023-02-23 23:23:00', 'tag_1': 12.3, 'tag_2': 121.44}
                       , {'timestamp': '2023-02-23 23:24:00', 'tag_1': None, 'tag_2': 421.44}
                       , {'timestamp': '2023-02-23 23:25:00', 'tag_1': None, 'tag_2': 321.44}
                       , {'timestamp': '2023-02-23 23:26:00', 'tag_1': 100.3, 'tag_2': 21.44}
                        ]
        spark_df_expected = spark.createDataFrame(expected_data)
        
        # Select the columns in the same order, and sort both dataframes before comparing them.
        spark_df_actual_sorted = spark_df_actual.select(['timestamp', 'tag_1', 'tag_2']).orderBy('timestamp')
        spark_df_expected_sorted = spark_df_expected.select(['timestamp', 'tag_1', 'tag_2']).orderBy('timestamp')

        self.assertTrue(is_actual_df_equal_to_expected_df(spark_df_actual_sorted, spark_df_expected_sorted) == True)
        
class FillTimeseriesXIntervalTests(unittest.TestCase):
    def test_fill_timeseries_x_interval_no_gaps_to_fill(self):
        data = [{'timestamp': '2023-02-23 23:23:00', 'tag_1': 1, 'tag_2': 12}
              , {'timestamp': '2023-02-23 23:24:00', 'tag_1': 2, 'tag_2': 33}
              , {'timestamp': '2023-02-23 23:25:00', 'tag_1': 3, 'tag_2': 102}
               ]
        spark_df_test = spark.createDataFrame(data)
        spark_df_actual = fill_timeseries_x_interval(spark_df_test, timeseries_column_name = 'timestamp', interval_minutes = 1)
        
        expected_data = data = [{'timestamp': '2023-02-23 23:23:00', 'tag_1': 1, 'tag_2': 12}
                              , {'timestamp': '2023-02-23 23:24:00', 'tag_1': 2, 'tag_2': 33}
                              , {'timestamp': '2023-02-23 23:25:00', 'tag_1': 3, 'tag_2': 102}
                               ]
        spark_df_expected = spark.createDataFrame(expected_data)
        
        # Select the columns in the same order, and sort both dataframes before comparing them.
        spark_df_actual_sorted = spark_df_actual.select(['timestamp', 'tag_1', 'tag_2']).orderBy('timestamp')
        spark_df_expected_sorted = spark_df_expected.select(['timestamp', 'tag_1', 'tag_2']).orderBy('timestamp')

        self.assertTrue(is_actual_df_equal_to_expected_df(spark_df_actual_sorted, spark_df_expected_sorted) == True)
                
    def test_fill_timeseries_x_interval_one_gap_to_fill(self):
        data = [{'timestamp': '2023-02-23 23:23:00', 'tag_1': 1, 'tag_2': 12}
              , {'timestamp': '2023-02-23 23:25:00', 'tag_1': 3, 'tag_2': 102}
               ]
        spark_df_test = spark.createDataFrame(data)
        spark_df_actual = fill_timeseries_x_interval(spark_df_test, timeseries_column_name = 'timestamp', interval_minutes = 1)
        
        expected_data = data = [{'timestamp': '2023-02-23 23:23:00', 'tag_1': 1, 'tag_2': 12}
                              , {'timestamp': '2023-02-23 23:24:00', 'tag_1': None, 'tag_2': None}
                              , {'timestamp': '2023-02-23 23:25:00', 'tag_1': 3, 'tag_2': 102}
                               ]
        spark_df_expected = spark.createDataFrame(expected_data)
        
        # Select the columns in the same order, and sort both dataframes before comparing them.
        spark_df_actual_sorted = spark_df_actual.select(['timestamp', 'tag_1', 'tag_2']).orderBy('timestamp')
        spark_df_expected_sorted = spark_df_expected.select(['timestamp', 'tag_1', 'tag_2']).orderBy('timestamp')

        self.assertTrue(is_actual_df_equal_to_expected_df(spark_df_actual_sorted, spark_df_expected_sorted) == True)

#     def test_fill_timeseries_x_interval_multiple_gaps_to_fill_hour(self):
#         data = [{'timestamp': '2023-02-23 13:23:00', 'tag_1': 1, 'tag_2': 12}
#               , {'timestamp': '2023-02-23 13:25:00', 'tag_1': 3, 'tag_2': 102}
#               , {'timestamp': '2023-02-23 13:23:00', 'tag_1': 3, 'tag_2': 102}
#               , {'timestamp': '2023-02-23 13:25:00', 'tag_1': 3, 'tag_2': 102}
#                 ]
#         spark_df_test = spark.createDataFrame(data)
#         spark_df_actual = fill_timeseries_x_interval(spark_df_test, timeseries_column_name = 'timestamp', interval_minutes = 60)
        
#         display(spark_df_actual)
        
#         expected_data = data = [{'timestamp': '2023-02-23 13:00:00', 'tag_1': 1, 'tag_2': 12}
#                               , {'timestamp': '2023-02-23 23:24:00', 'tag_1': None, 'tag_2': None}
#                               , {'timestamp': '2023-02-23 23:25:00', 'tag_1': 3, 'tag_2': 102}
#                               , {'timestamp': '2023-02-24 23:23:00', 'tag_1': 3, 'tag_2': 102}
#                               , {'timestamp': '2023-02-24 23:24:00', 'tag_1': None, 'tag_2': None}
#                               , {'timestamp': '2023-02-24 23:25:00', 'tag_1': 3, 'tag_2': 102}
#                                 ]
#         spark_df_expected = spark.createDataFrame(expected_data)
        
#         # Select the columns in the same order, and sort both dataframes before comparing them.
#         spark_df_actual_sorted = spark_df_actual.select(['timestamp', 'tag_1', 'tag_2']).orderBy('timestamp')
#         spark_df_expected_sorted = spark_df_expected.select(['timestamp', 'tag_1', 'tag_2']).orderBy('timestamp')

#         self.assertTrue(is_actual_df_equal_to_expected_df(spark_df_actual_sorted, spark_df_expected_sorted) == True)

class RollupAndAggProcessDataXMinTests(unittest.TestCase):
    def test_rollup_and_agg_process_data_x_min_with_multiple_data(self):
        data = [{'timestamp': '2023-02-23 23:23:00', 'tag_1': 12.3, 'tag_2': 121.44, 'tag_3': 23.0}
              , {'timestamp': '2023-02-23 22:24:00', 'tag_1': 1.3, 'tag_2': 421.44, 'tag_3': 121.0}
              , {'timestamp': '2023-02-23 23:25:00', 'tag_1': 112.3, 'tag_2': 221.44, 'tag_3': 2.0}
              , {'timestamp': '2023-02-23 22:26:00', 'tag_1': 12.3, 'tag_2': 121.44, 'tag_3': 34.0}
              , {'timestamp': '2023-02-24 23:26:00', 'tag_1': 10.0, 'tag_2': 21.44, 'tag_3': 89.0}
              , {'timestamp': '2023-02-24 22:26:00', 'tag_1': 100.0, 'tag_2': 200.44, 'tag_3': 464.0}
               ]
        spark_df_test = spark.createDataFrame(data)
        tag_agg_dict = {'tag_1': 'max'
                      , 'tag_2': 'mean'
                      , 'tag_3': 'sum'
                       }
        spark_df_actual = rollup_and_agg_process_data_x_min(spark_df_test, tag_agg_dict)
        
        expected_data = [{'timestamp': '2023-02-23 22:00:00', 'tag_1': 12.3, 'tag_2': 271.44, 'tag_3': 155.0}
                       , {'timestamp': '2023-02-23 23:00:00', 'tag_1': 112.3, 'tag_2': 171.44, 'tag_3': 25.0}
                       , {'timestamp': '2023-02-24 23:00:00', 'tag_1': 10.0, 'tag_2': 21.44, 'tag_3': 89.0}
                       , {'timestamp': '2023-02-24 22:00:00', 'tag_1': 100.0, 'tag_2': 200.44, 'tag_3': 464.0}
                        ]
        
        spark_df_expected = spark.createDataFrame(expected_data)
                
        # Select the columns in the same order, and sort both dataframes before comparing them.
        spark_df_actual_sorted = spark_df_actual.select(['timestamp', 'tag_1', 'tag_2', 'tag_3']).orderBy('timestamp')
        spark_df_expected_sorted = spark_df_expected.select(['timestamp', 'tag_1', 'tag_2', 'tag_3']).orderBy('timestamp')

        self.assertTrue(is_actual_df_equal_to_expected_df(spark_df_actual_sorted, spark_df_expected_sorted) == True)

# COMMAND ----------

# A simple way to check. Definitely not the best because it fails when there are duplicates!
def is_actual_df_equal_to_expected_df(df_actual, df_expected):
    if df_actual.subtract(df_expected).rdd.isEmpty():
        return df_expected.subtract(df_actual).rdd.isEmpty()
    return False
