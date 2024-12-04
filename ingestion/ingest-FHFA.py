# Databricks notebook source
## This file will read the data source at https://www.fhfa.gov/data/hpi/datasets
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

# Get the data from the Master Dataset
url = “https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv”

response = requests.get(url)
response.raise_for_status()

file_content = BytesIO(response.content)
master_df = pd.read_csv(file_content)


# COMMAND ----------

# # Get the data from the another dataset
# # Currently unused as other sources from the FHFA have not been analyzed yet
# url = “https://files.zillowstatic.com/research/public_csvs/market_temp_index/Metro_market_temp_index_uc_sfrcondo_month.csv?t=1733285389”

# response = requests.get(url)
# response.raise_for_status()

# file_content = BytesIO(response.content)
# sales_df = pd.read_csv(file_content)


# COMMAND ----------

## Upload datasets to Azure Storage

