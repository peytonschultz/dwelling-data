# Databricks notebook source
## This file will read the data source at https://www.zillow.com/research/data/
# This must be done via web scraping as there is no way to request the data via API
# This data updates quarterly and therefore this script must be scheduled to run quarterly

# COMMAND ----------

# Sample Code given br Mr. Froslie:
#import requests
#import pandas as pd
#from io import BytesIO

#url = “Insert appropriate URL here”

#response = requests.get(url)
#response.raise_for_status()

#file_content = BytesIO(response.content)
#df = pd.read_csv(file_content)

# From here, save to CSV and upload to Azure.



# COMMAND ----------

# Handle Imports
import requests
import pandas as pd
from io import BytesIO


# COMMAND ----------

# Get the data from the ZHVI Dataset
url = “https://files.zillowstatic.com/research/public_csvs/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1733285388”

response = requests.get(url)
response.raise_for_status()

file_content = BytesIO(response.content)
index_df = pd.read_csv(file_content)


# COMMAND ----------

# Get the data from the ZHVI Dataset
url = “https://files.zillowstatic.com/research/public_csvs/market_temp_index/Metro_market_temp_index_uc_sfrcondo_month.csv?t=1733285389”

response = requests.get(url)
response.raise_for_status()

file_content = BytesIO(response.content)
sales_df = pd.read_csv(file_content)


# COMMAND ----------

## Upload both datasets to Azure Storage

