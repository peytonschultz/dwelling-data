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

key = "zok14WxkLHcnO4XO5CDnICknOksOzb5FFCKArpzuR0no5fIXyZqgylbZoRWJXZu3/Ew8z6eOgL7A+AStevcwOQ=="
save_file_to_ADLS("../temp_data/zillow-ZHVI.csv", "zillow-ZHVI.csv", "storeassignment1", key, "raw-data")
save_file_to_ADLS("../temp_data/zillow-MarketHeat.csv", "zillow-MarketHeat.csv", "storeassignment1", key, "raw-data")

# COMMAND ----------



# Melt the DataFrame
index_df_melted = index_df.melt(id_vars=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName'], 
                                var_name='Date', 
                                value_name='Value')

# Convert 'Date' to datetime
index_df_melted['Date'] = pd.to_datetime(index_df_melted['Date'])

# Extract year and month
index_df_melted['Year'] = index_df_melted['Date'].dt.year
index_df_melted['Month'] = index_df_melted['Date'].dt.month

display(index_df_melted)

# COMMAND ----------

# Melt the DataFrame
heat_value_df_melted = market_df.melt(id_vars=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName'], 
                                var_name='Date', 
                                value_name='Value')

# Convert 'Date' to datetime
heat_value_df_melted['Date'] = pd.to_datetime(heat_value_df_melted['Date'])

# Extract year and month
heat_value_df_melted['Year'] = heat_value_df_melted['Date'].dt.year
heat_value_df_melted['Month'] = heat_value_df_melted['Date'].dt.month

display(heat_value_df_melted)

# COMMAND ----------

# Join the DataFrames on 'RegionID'
joined_df = heat_value_df_melted.merge(index_df_melted, on='RegionID', suffixes=('', '_index'))

# Drop records where 'Date_heat' and 'Date_index' don't match
joined_df = joined_df[joined_df['Date'] == joined_df['Date_index']]

# Drop duplicate columns
joined_df = joined_df.drop(columns=['SizeRank_index', 'RegionName_index','RegionType_index','StateName_index', 'Date_index','Year_index', 'Month_index'])

# Rename columns
joined_df = joined_df.rename(columns={'Value': 'Heat index', 'Value_index': 'ZHVI'})

display(joined_df)

# COMMAND ----------

# Create a new 'ZHVI_Delta' column that represents the change in ZHVI index between two months
joined_df['ZHVI_Delta'] = joined_df.groupby('RegionName')['ZHVI'].diff().fillna(0.0)

# Create a new 'Heat_Delta' column that represents the change in ZHVI index between two months
joined_df['Heat_Delta'] = joined_df.groupby('RegionName')['Heat index'].diff().fillna(0)

display(joined_df)

# COMMAND ----------

## Upload both dataframes to temp-data
index_df_melted.to_csv("../temp_data/zillow-ZHVI-clean.csv")
heat_value_df_melted.to_csv("../temp_data/zillow-MarketHeat-clean.csv")
joined_df.to_csv("../temp_data/zillow-joined-transform.csv")

# Save to Azure
save_file_to_ADLS("../temp_data/zillow-ZHVI-clean.csv", "zillow-ZHVI-clean.csv", "storeassignment1", key, "clean-data")
save_file_to_ADLS("../temp_data/zillow-MarketHeat-clean.csv", "zillow-MarketHeat-clean.csv", "storeassignment1", key, "clean-data")
save_file_to_ADLS("../temp_data/zillow-joined-transform.csv", "zillow-joined-transform.csv", "storeassignment1", key, "transformed-data")

