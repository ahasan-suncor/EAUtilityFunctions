# Databricks notebook source
import unittest

class LoadDataFromPathTests(unittest.TestCase):
    
    def setUp(self):
        self.data_path = '/tmp/TestLoadDataFromPath/TestData'
        data = [{'first_name': 'Jane', 'last_name': 'Doe', 'age': 10}
              , {'first_name': 'Play', 'last_name': 'Doe', 'age': 20}
              , {'first_name': 'Taekwon', 'last_name': 'Doe', 'age': 35}
               ]
        self.spark_df_test = spark.createDataFrame(data)
        self.spark_df_test.write.format('csv').option('header', True).mode('overwrite').save(self.data_path)

    @classmethod
    def tearDown(cls):
        dbutils.fs.rm('/tmp/TestLoadDataFromPath/TestData', True)

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

class AddColumnPrefixTests(unittest.TestCase):
    
    def setUp(self):
        self.spark_df_test = spark.createDataFrame([('Jane', 'Doe', 10), ('Play', 'Doe', 20), ('Taekwon', 'Doe', 35)]
                                                 , ['first_name', 'last_name', 'age'])

    def test_add_column_prefix_with_prefix(self):
        prefixed_df = add_column_prefix(self.spark_df_test, 'human_')
        self.assertEqual(prefixed_df.columns, ['human_first_name', 'human_last_name', 'human_age'])

    def test_add_column_prefix_empty_prefix(self):
        prefixed_df = add_column_prefix(self.spark_df_test, '')
        self.assertEqual(prefixed_df.columns, ['first_name', 'last_name', 'age'])

class AddAuditColsToSparkDFTests(unittest.TestCase):
    
    def setUp(self):
        data = [{'first_name': 'Jane', 'last_name': 'Doe', 'age': 10}
              , {'first_name': 'Play', 'last_name': 'Doe', 'age': 20}
              , {'first_name': 'Taekwon', 'last_name': 'Doe', 'age': 35}
               ]
        self.spark_df_test = self.spark_df_test = spark.createDataFrame(data)

    def test_add_audit_cols_to_spark_df(self):
        spark_df_actual = add_audit_cols_to_spark_df(self.spark_df_test)
        actual_cols = spark_df_actual.columns
        expected_cols = ['age', 'first_name', 'last_name', 'CreatedBy', 'CreatedDateTime_UTC']
        self.assertEqual(actual_cols, expected_cols)

    def test_add_audit_cols_to_spark_df_multiple(self):
        spark_df_with_audit_cols = add_audit_cols_to_spark_df(self.spark_df_test)
        spark_df_actual = add_audit_cols_to_spark_df(spark_df_with_audit_cols)
        actual_cols = spark_df_actual.columns
        expected_cols = ['age', 'first_name', 'last_name', 'CreatedBy', 'CreatedDateTime_UTC', 'CreatedBy', 'CreatedDateTime_UTC']
        self.assertEqual(actual_cols, expected_cols)           

class AddBusinessKeyHashValueToSparkDFTests(unittest.TestCase):
    
    def setUp(self):
        data = [{'first_name': 'Jane', 'last_name': 'Doe', 'age': 10}
              , {'first_name': 'Play', 'last_name': 'Doe', 'age': 20}
              , {'first_name': 'Taekwon', 'last_name': 'Doe', 'age': 35}
               ]
        self.spark_df_test = self.spark_df_test = spark.createDataFrame(data)
        self.business_key_cols = ['first_name', 'last_name']

    def test_add_business_key_hash_value_to_spark_df_drop_duplicates(self):
        df_with_dups = self.spark_df_test.union(self.spark_df_test)
        spark_df_with_hash = add_business_key_hash_value_to_spark_df(df_with_dups, self.business_key_cols)
        self.assertEqual(spark_df_with_hash.count(), self.spark_df_test.count())
        
    def test_add_business_key_hash_value_to_spark_df_business_key_cols(self):
        spark_df_with_hash = add_business_key_hash_value_to_spark_df(self.spark_df_test, self.business_key_cols)
        expected_columns = self.business_key_cols + ['age', 'BusinessKeyColHash']
        self.assertEqual(set(spark_df_with_hash.columns), set(expected_columns))
        
    def test_add_business_key_hash_value_to_spark_df_hash_value(self):
        spark_df_with_hash = add_business_key_hash_value_to_spark_df(self.spark_df_test, self.business_key_cols)
        actual_hash_vals = spark_df_with_hash.select('BusinessKeyColHash').rdd.flatMap(lambda x: x).collect()
        # https://www.md5hashgenerator.com/
        expected_hash_vals = ['64d59c83967ff70ad33fb6142bd8c902', 'ba4985621d3c63437e780bdb05a8bd60', '325c855071342ecfcdd3478a46d90fce']
        self.assertEqual(set(actual_hash_vals), set(expected_hash_vals))

class SaveSparkDFToPathTests(unittest.TestCase):

    def setUp(self):
        self.save_path = '/tmp/SaveDFToPath/TestData'
        data = [{'first_name': 'Jane', 'last_name': 'Doe', 'age': 10}
              , {'first_name': 'Play', 'last_name': 'Doe', 'age': 20}
              , {'first_name': 'Taekwon', 'last_name': 'Doe', 'age': 35}
               ]
        self.spark_df_test = spark_df_test = spark.createDataFrame(data)

    @classmethod
    def tearDown(cls):
        dbutils.fs.rm('/tmp/SaveDFToPath/TestData', True)

    def test_save_spark_df_to_path_as_new_delta_table(self):
        save_spark_df_to_path(self.spark_df_test, save_path = self.save_path)
        spark_df_loaded = spark.read.format('delta').load(self.save_path)
        self.assertTrue(spark_df_loaded.count() == 3)
        self.assertTrue(len(spark_df_loaded.columns) == 3)

    def test_save_spark_df_to_path_with_merge(self):
        # Save as new delta table first. **** Add hash column. **** 
        spark_df_with_hash_val = add_business_key_hash_value_to_spark_df(self.spark_df_test, ['first_name', 'last_name'])
        save_spark_df_to_path(spark_df_with_hash_val, save_path = self.save_path)
        
        # New data is merged with the data that already exists.
        new_data = [{'first_name': 'Jane', 'last_name': 'Doe', 'age': 100}
                  , {'first_name': 'Play', 'last_name': 'Doe', 'age': 200}
                  , {'first_name': 'Taekwon', 'last_name': 'Doe', 'age': 350}
                   ]
        spark_df_new = spark.createDataFrame(new_data)
        spark_df_new_with_hash_val = add_business_key_hash_value_to_spark_df(spark_df_new, ['first_name', 'last_name'])
        save_spark_df_to_path(spark_df_new_with_hash_val, save_path = self.save_path)
        
        spark_df_loaded = spark.read.format('delta').load(self.save_path)
        self.assertTrue(spark_df_loaded.count() == 3)
        self.assertTrue(len(spark_df_loaded.columns) == 4)
