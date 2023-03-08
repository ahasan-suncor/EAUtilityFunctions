# Databricks notebook source
# MAGIC %run ./io_functions

# COMMAND ----------

# MAGIC %run ./io_functions_test

# COMMAND ----------

r = unittest.main(argv = [''], verbosity = 2, exit = False)
assert r.result.wasSuccessful(), 'Test failed :(. See log above.'
