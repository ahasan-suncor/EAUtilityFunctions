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
