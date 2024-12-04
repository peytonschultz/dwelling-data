# Databricks notebook source
## This file will read the kaggle dataset located at https://www.kaggle.com/datasets/yasserh/housing-prices-dataset


# COMMAND ----------

import pandas as pd
import kagglehub

# COMMAND ----------

# Download latest version
path = kagglehub.dataset_download("yasserh/housing-prices-dataset")

# print("Path to dataset files:", path)
house_df = pd.read_csv(path)


# COMMAND ----------

# Upload Dataset to Azure Storage

