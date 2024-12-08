# Databricks notebook source
# The first step is to connect to your storage account after setting up a Databricks secret scope and a key vault.  Follow the PDF walkthrough to get this set up.
# You can use the storage account from the first assignment (GitHub and Azure) for this assignment.

# You'll need to replace these values with the specifics for your setup.

storage_end_point = "dwelling-data.dfs.core.windows.net" 
my_scope = "MarchMadnessScope"
my_key = "assign1-key"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

# Replace the container name (assign-1-blob) and storage account name (assign1storage) in the uri.
uri = "abfss://assign-1-blob@assign1storage.dfs.core.windows.net/"


# COMMAND ----------

# Read the data file from the storage account.  This the same datafile used in assignment 1.  It is also available in the InputData folder of this assignment's repo.
sp_df = spark.read.csv(uri+'SandP500Daily.csv', header=True)
 
display(sp_df)
