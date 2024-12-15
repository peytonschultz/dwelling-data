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

#Not secure, should change with a config file once I find this out
key = "taMGH41OKScywtit3Ue6DXXMt/ikOZZcu6UghF0YIbQl7Obh77wbW9MzCouej0ils/d7Wod2OcvT+AStYLVj/Q=="

# COMMAND ----------

# Handle Imports
import requests
import pandas as pd
from io import BytesIO
from azure.storage.filedatalake import DataLakeServiceClient


# COMMAND ----------

# Get the data from the ZHVI Dataset
url = "https://files.zillowstatic.com/research/public_csvs/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1733285388"

response = requests.get(url)
response.raise_for_status()

file_content = BytesIO(response.content)
index_df = pd.read_csv(file_content)
display(index_df)


# COMMAND ----------

# Get the data from the ZHVI Dataset
url = "https://files.zillowstatic.com/research/public_csvs/market_temp_index/Metro_market_temp_index_uc_sfrcondo_month.csv?t=1733285389"

response = requests.get(url)
response.raise_for_status()

file_content = BytesIO(response.content)
market_df = pd.read_csv(file_content)
display(market_df)


# COMMAND ----------

## Upload both datasets to temp_data
index_df.to_csv("../temp_data/zillow-ZHVI.csv")
market_df.to_csv("../temp_data/zillow-MarketHeat.csv")

#pd.save_table(index_df, "temp_data", "zillow-index")
#pd.save_table(market_df, "temp_data", "zillow-heat-index")



# COMMAND ----------

# Method to connect to a storage account with an account key. Taken from the example in github
def initialize_storage_account(storage_account_name, storage_account_key):
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)


# COMMAND ----------

from azure.core.exceptions import ResourceExistsError

def save_file_to_ADLS(file_path, file_name, storage_account_name, storage_account_key, azure_directory):
    initialize_storage_account(storage_account_name, storage_account_key)
      
    try:
        file_system_client = service_client.create_file_system(file_system="dwelling-data")
    except ResourceExistsError:
        file_system_client = service_client.get_file_system_client(file_system="dwelling-data")
    
    directory_client = file_system_client.create_directory(azure_directory)
   
    directory_client = file_system_client.get_directory_client(azure_directory)
    file_client = directory_client.create_file(file_name)

    file = open(file_path,'r')
    contents = file.read()
    file_client.upload_data(contents, overwrite=True)

# COMMAND ----------


save_file_to_ADLS("../temp_data/zillow-ZHVI.csv", "zillow-ZHVI.csv", "storeassignment1", key, "raw-data")
save_file_to_ADLS("../temp_data/zillow-MarketHeat.csv", "zillow-MarketHeat.csv", "storeassignment1", key, "raw-data")
