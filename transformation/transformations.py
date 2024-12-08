# Databricks notebook source


# COMMAND ----------

# List directories in the /Users/ path
dbutils.fs.ls("/")

# COMMAND ----------

df = spark.read.csv("dbfs:/temp_data/raw-data/zillow_ZHVI.csv", header=True, inferSchema=True)
display(df)
