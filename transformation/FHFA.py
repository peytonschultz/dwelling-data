import pandas as pd
import os

path = 'temp_data/hpi_master.csv'

if os.path.exists(path):
    print("File found!")
else:
    print("File not found. Check the path and file name.")

zillow_df = pd.read_csv(path)

print(zillow_df)



