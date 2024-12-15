# Databricks notebook source
import requests
import pandas as pd
import pyspark.pandas as ps
from io import BytesIO
from azure.storage.filedatalake import DataLakeServiceClient

# COMMAND ----------

storage_end_point = "storeassignment1.dfs.core.windows.net" 
my_scope = "dwelling-data-project"
my_key = "dwelling-data-key"


# COMMAND ----------

spark.conf.set("fs.azure.account.key." + storage_end_point, dbutils.secrets.get(scope = my_scope, key = my_key))
uri = "abfss://dwelling-data@storeassignment1.dfs.core.windows.net"
url = "https://storeassignment1.blob.core.windows.net/dwelling-data"

# COMMAND ----------

#load dataframes from azure storage
index_df = spark.read.csv(uri+'/raw-data/zillow-ZHVI.csv', header=True)
market_df = spark.read.csv(uri+'/raw-data/zillow-MarketHeat.csv', header=True)

# COMMAND ----------

# Drop the extra column if it exists
if '_c0' in index_df.columns:
    index_df = index_df.drop('_c0')

# Convert the Spark DataFrame to a Pandas DataFrame
index_df_pd = index_df.toPandas()

# Melt the DataFrame
index_df_melted = pd.melt(
    index_df_pd,
    id_vars=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName'],
    var_name='Date',
    value_name='Value'
)

# Convert 'Date' to datetime
index_df_melted['Date'] = pd.to_datetime(index_df_melted['Date'], errors='coerce')

# Extract year and month
index_df_melted['Year'] = index_df_melted['Date'].dt.year
index_df_melted['Month'] = index_df_melted['Date'].dt.month

display(index_df_melted)

# COMMAND ----------


# Drop the extra column if it exists
if '_c0' in market_df.columns:
    market_df = market_df.drop('_c0')

# Convert the Spark DataFrame to a Pandas DataFrame
market_df_pd = market_df.toPandas()

# Melt the DataFrame
market_df_melted = pd.melt(
    market_df_pd,
    id_vars=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName'],
    var_name='Date',
    value_name='Value'
)

# Convert 'Date' to datetime
market_df_melted['Date'] = pd.to_datetime(market_df_melted['Date'], errors='coerce')

# Extract year and month
market_df_melted['Year'] = market_df_melted['Date'].dt.year
market_df_melted['Month'] = market_df_melted['Date'].dt.month

display(market_df_melted)

# COMMAND ----------

# Ensure numeric columns are numeric on index_df_melted
numeric_columns_index = index_df_melted.columns.difference(['RegionName', 'RegionType', 'StateName', 'Date'])
index_df_melted[numeric_columns_index] = index_df_melted[numeric_columns_index].apply(pd.to_numeric, errors='coerce')

# Ensure numeric columns are numeric on market_df_melted
numeric_columns_market = market_df_melted.columns.difference(['RegionName', 'RegionType', 'StateName', 'Date'])
market_df_melted[numeric_columns_market] = market_df_melted[numeric_columns_market].apply(pd.to_numeric, errors='coerce')

display(index_df_melted)
display(market_df_melted)

# COMMAND ----------

# Join the DataFrames on 'RegionID'
joined_df = market_df_melted.merge(index_df_melted, on='RegionID', suffixes=('', '_index'))

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

from azure.core.exceptions import ResourceExistsError
# Method to connect to a storage account with an account key. Taken from the example in github
def initialize_storage_account(storage_account_name, storage_account_key):
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)

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

key = ""
with open("../key.config") as f:
    key = f.readline()


# COMMAND ----------


## Upload both dataframes to temp-data
index_df_melted.to_csv("../temp_data/zillow-ZHVI-clean.csv")
market_df_melted.to_csv("../temp_data/zillow-MarketHeat-clean.csv")
joined_df.to_csv("../temp_data/zillow-joined-transform.csv")

# Save to Azure
save_file_to_ADLS("../temp_data/zillow-ZHVI-clean.csv", "zillow-ZHVI-clean.csv", "storeassignment1", key, "clean-data")
save_file_to_ADLS("../temp_data/zillow-MarketHeat-clean.csv", "zillow-MarketHeat-clean.csv", "storeassignment1", key, "clean-data")
save_file_to_ADLS("../temp_data/zillow-joined-transform.csv", "zillow-joined-transform.csv", "storeassignment1", key, "transformed-data")
