import pandas as pd

# 1. Load data from the two files
expanded_df = pd.read_csv('expanded.csv')
res_df = pd.read_csv('res.csv')

# 2. Handle cases where titles and names are split across columns
# We prioritize 'title'; if 'title' is missing (NaN/Null), we take the value from 'name'
expanded_df['final_title'] = expanded_df['title'].fillna(expanded_df['name'])

# Note: If both columns contain data and you want to keep both, 
# you could concatenate them. Here, we assume they represent 
# the same entity, so we use fillna to get a representative value.

# 3. Select necessary columns and remove duplicates based on channel_id
# This ensures we don't create redundant rows during the merge
info_df = expanded_df[['channel_id', 'category', 'final_title']].drop_duplicates(subset=['channel_id'])

# 4. Merge the extracted info into the main results dataframe
result_df = pd.merge(res_df, info_df, on='channel_id', how='left')

# 5. Rename 'final_title' back to 'title' to match the desired format
result_df = result_df.rename(columns={'final_title': 'title'})

# 6. Save the final result to a new CSV file
result_df.to_csv('res_updated.csv', index=False)
print("Category and Title have been successfully added!")
