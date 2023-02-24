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
