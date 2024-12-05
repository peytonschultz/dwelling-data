import pandas as pd
import os

path = 'temp_data/Housing.csv'

if os.path.exists(path):
    print("File found!")
else:
    print("File not found. Check the path and file name.")

kaggle_df = pd.read_csv(path)

# Remove entries with empty data
kaggle_df_clean = kaggle_df.dropna()

# changing yes/no columns to 1/0 columns
target_columns = ['mainroad','guestroom','basement','hotwaterheating','airconditioning','parking','prefarea']

kaggle_df_clean[target_columns] = kaggle_df_clean[target_columns].replace({'yes':1,'no':0})

# change furnishing status to numerical values (furnished = 2, semi-furnished = 1, unfurnished = 0)
kaggle_df_clean['furnishingstatus'] = kaggle_df_clean['furnishingstatus'].replace({'furnished': 2, 'semi-furnished': 1, 'unfurnished': 0})

print(kaggle_df_clean)

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Load the dataset (assuming df is already preprocessed)
X = kaggle_df_clean.drop('price', axis=1)  # Features
y = kaggle_df_clean['price']  # Target variable

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Create and train the regression model
model = LinearRegression()
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

# Evaluate the model
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

# Print the evaluation metrics
print(f"Mean Absolute Error (MAE): {mae}")
print(f"Mean Squared Error (MSE): {mse}")
print(f"R-squared (R²): {r2}")
