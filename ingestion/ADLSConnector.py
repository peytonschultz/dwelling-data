# Databricks notebook source
# This code file should contain the code required to upload data to Azure Data Lake Storage.
# The problem should be able to run by the instructor using 'python ingestREST.py' (with their own API key).
# Store your key in a file with a .config appendix to avoid accidental check-in (.gitignore is setup to ignore .config)

# Assignment problems:
#   1a) Upload the PopulationWithPercentIncrease.csv file from the IngestKaggle.py problem to the storage account created 
#       for assignment 1.  Programmatically create a new container, assign-2, and a new directory,
#       upload-sample before uploading.
#   1b) Take a screenshot of the uploaded file in your storage account, showing the full path.
#   1c) Save the screenshot to the Data\Upload folder in git and push to GitHub.

# %%
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

# %%
# Method to connect to a storage account with an account key. Taken from the example in github
def initialize_storage_account(storage_account_name, storage_account_key):
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)


#%%
storage_account_name = "storeassignment1"

# Use .gitignore to avoid accidental submission.
with open("azureKey.config") as f:
    storage_account_key=f.readline()

initialize_storage_account(storage_account_name, storage_account_key)

print(service_client)

#%%
# Create a container and a directory.
file_system_client = service_client.create_file_system(file_system="assign-2")    

directory_client=file_system_client.create_directory("upload-sample")


#%%
# Upload the data file.
directory_client = file_system_client.get_directory_client("upload-sample")
        
file_client = directory_client.create_file("PopulationWithPercentIncrease.csv")

population_file = open("Data\WorldPopulation\PopulationWithPercentIncrease.csv",'r')

file_contents = population_file.read()

file_client.upload_data(file_contents, overwrite=True)

# %%

# COMMAND ----------

def save_file_to_ADLS(file_path, file_name, storage_account_name, storage_account_key, azure_directory):
    initialize_storage_account(storage_account_name, storage_account_key)
    file_system_client = service_client.create_file_system(file_system="dwelling-data")
    directory_client = file_system_client.create_directory(azure_directory)

    directory_client = file_system_client.get_directory_client(azure_directory)
    file_client = directory_client.create_file(file_name)

    file = open(file_path,'r')
    contents = file.read()
    file_client.upload_data(contents, overwrite=True)




