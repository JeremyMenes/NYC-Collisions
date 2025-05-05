import pandas as pd
import glob
import os

# Directory containing your CSV files
csv_dir = r'C:\Users\menes\Documents\NYC Traffic Collisions Project\Weather Data\weather stations batched'

# Get all CSV file paths
all_files = glob.glob(os.path.join(csv_dir, "*.csv"))

# List to collect DataFrames
df_list = []

# Read each CSV into a DataFrame
for file in all_files:
    df = pd.read_csv(file)  # Let pandas infer dtypes (floats will be preserved)
    df_list.append(df)

# Concatenate all DataFrames into one
combined_df = pd.concat(df_list, ignore_index=True)

# Save combined DataFrame to a new CSV
combined_df.to_csv('combined_csv.csv', index=False)