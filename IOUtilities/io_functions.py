# Databricks notebook source
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import md5, concat_ws
from delta.tables import DeltaTable

def load_data_from_path(data_path: str, data_format: str = 'delta', data_options: dict = {}) -> SparkDataFrame:
    """
    Loads data from a specified path using PySpark.

    Args:
        data_path: A string specifying the path to the data.
        data_format: A string specifying the format of the data.
        data_options: A dictionary of options to pass to the PySpark reader.

    Returns:
        A PySpark DataFrame containing the loaded data.
        Returns None if data_path is None.
    """

    if data_path is None:
        return None

    return spark.read \
                .format(data_format) \
                .options(**data_options) \
                .load(data_path)

def add_column_prefix(spark_df: SparkDataFrame, prefix_str: str) -> SparkDataFrame:
    """
    Adds a prefix string to the names of all columns in a PySpark DataFrame.

    Args:
        spark_df: A PySpark DataFrame.
        prefix_str: A string to add as a prefix to all column names.

    Returns:
        A new PySpark DataFrame with all column names prefixed.
    """

    for df_column in spark_df.columns:
        spark_df = spark_df.withColumnRenamed(df_column, '{}{}'.format(prefix_str, df_column))
    return spark_df

def add_audit_cols_to_spark_df(spark_df: SparkDataFrame) -> SparkDataFrame:
    """
    This function adds audit columns to a Spark DataFrame regarding row creation.

    Args:
        spark_df: The Spark DataFrame to add the hash column to.

    Returns:
        SparkDataFrame: The Spark DataFrame with audit columns added
    """

    sdf_audit_cols = spark.sql("""
                               SELECT CURRENT_USER AS CreatedBy
                                    , CURRENT_TIMESTAMP AS CreatedDateTime_UTC
                               """)

    return spark_df.crossJoin(sdf_audit_cols)

def add_business_key_hash_value_to_spark_df(spark_df: SparkDataFrame, business_key_cols: list) -> SparkDataFrame:
    """
    This function adds a new column to a Spark DataFrame that contains a hash value of the business key columns.
    The resulting hash is used to uniquely identify the record based on the business key.

    Args:
        spark_df: The Spark DataFrame to add the hash column to.
        business_key_cols: A list of column names to use as the business key columns.

    Returns:
        SparkDataFrame: The Spark DataFrame with the new hash column added.
    """

    # We drop the duplicates because the key will be a unique identfier for each record.
    values_seperator = ':'
    return spark_df.dropDuplicates(business_key_cols) \
                   .withColumn('BusinessKeyColHash', md5(concat_ws(values_seperator, *business_key_cols)))

def save_spark_df_to_path(spark_df: SparkDataFrame, save_path: str):
    """
    This function saves a dataframe to the specified path using PySpark.

    Args:
        spark_df: The Spark DataFrame to add save.
        save_path: A string specifying the path to save the data.

    Assumptions:
        Data is saved in a delta format.
        The save mode is either as a new table or a merge.
        'BusinessKeyColHash' column exists ('add_business_key_hash_value_to_spark_df' was run)
    """

    is_delta_table_in_save_path = DeltaTable.isDeltaTable(spark, save_path)
    if is_delta_table_in_save_path:
        __save_spark_df_with_merge(spark_df, save_path)
    else:
        __save_spark_df_as_new_delta_table(spark_df, save_path)
        
def __save_spark_df_with_merge(spark_df: SparkDataFrame, save_path: str):
    DeltaTable.forPath(spark, save_path).alias('persisted') \
              .merge(spark_df.alias('incoming'), 'persisted.BusinessKeyColHash = incoming.BusinessKeyColHash') \
              .whenMatchedUpdateAll() \
              .whenNotMatchedInsertAll() \
              .execute()
    
def __save_spark_df_as_new_delta_table(spark_df: SparkDataFrame, save_path: str):
    spark_df.write \
            .mode('overwrite') \
            .format('delta') \
            .save(save_path)
