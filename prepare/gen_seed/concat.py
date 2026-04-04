import pandas as pd
import glob

# 1. List all files matching the pattern
file_pattern = 'vietnam_channels*.csv'
files = glob.glob(file_pattern)

# 2. Read and concatenate all CSV files
df_list = [pd.read_csv(file) for file in files]
combined_df = pd.concat(df_list, ignore_index=True)

# 3. Remove duplicates based on 'channel_id' and keep the first occurrence
df_cleaned = combined_df.drop_duplicates(subset=['channel_id'], keep='first')

# Output verification
print(f"Total files processed: {len(files)}")
print(f"Rows before filtering: {len(combined_df)}")
print(f"Rows after filtering unique channel_id: {len(df_cleaned)}")

# Optional: Save the cleaned data
df_cleaned.to_csv('unique.csv', index=False)
