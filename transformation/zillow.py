import pandas as pd
import os

# path = 'temp_data/Zillow_ZHVI.csv'

# if os.path.exists(path):
#     print("File found!")
# else:
#     print("File not found. Check the path and file name.")

# zillow_df = pd.read_csv(path)

# print(zillow_df)

#zillow_df = spark.read.csv("https://storeassignment1.blob.core.windows.net/dwelling-data/raw-data/zillow-ZHVI.csv", header=True, inferSchema=True)

storage_end_point = "storeassignment1.blob.core.windows.net" 
my_scope = "dwelling-data"
my_key = "zok14WxkLHcnO4XO5CDnICknOksOzb5FFCKArpzuR0no5fIXyZqgylbZoRWJXZu3/Ew8z6eOgL7A+AStevcwOQ=="

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

# Replace the container name (assign-1-blob) and storage account name (assign1storage) in the uri.
uri = "abfss://dwelling-data@storeassignment1.blob.core.windows.net/"

sp_df = spark.read.csv(uri+'SandP500Daily.csv', header=True)
 
display(sp_df)

