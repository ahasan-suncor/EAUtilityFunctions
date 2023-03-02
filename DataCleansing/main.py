# Databricks notebook source
# MAGIC %run ./cleansing_functions

# COMMAND ----------

# MAGIC %run ./cleansing_functions_test

# COMMAND ----------

r = unittest.main(argv = [''], verbosity = 2, exit = False)
assert r.result.wasSuccessful(), 'Test failed :(. See log above.'

# COMMAND ----------



# COMMAND ----------


