# Databricks notebook source
# MAGIC %run ./data_aggregation_functions

# COMMAND ----------

# MAGIC %run ./data_aggregation_functions_test

# COMMAND ----------

r = unittest.main(argv = [''], verbosity = 2, exit = False)
assert r.result.wasSuccessful(), 'Test failed :(. See log above.'
