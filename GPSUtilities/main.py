# Databricks notebook source
# MAGIC %run ./gps_functions

# COMMAND ----------

# MAGIC %run ./gps_functions_test

# COMMAND ----------

r = unittest.main(argv = [''], verbosity = 2, exit = False)
assert r.result.wasSuccessful(), 'Test failed :(. See log above.'
