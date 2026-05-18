import pandas as pd
import os

# 1. Define paths to the general CSV files
file_paths = {
    'mBERT': 'mbert/m_bert_general.csv',
    'phoBERT': 'phobert/pho_bert_general.csv',
    'mModernBERT': 'mmodernbert/m_modern_bert_general.csv'
}

dataframes = []

# 2. Read each file and append to the list
for model_name, path in file_paths.items():
    if os.path.exists(path):
        df = pd.read_csv(path)
        
        # Insert 'Model' column at the beginning to distinguish the data
        df.insert(0, 'Model', model_name)
        
        dataframes.append(df)
        print(f"Successfully read: {path}")
    else:
        print(f"Warning: File not found at {path}")

# 3. Concatenate and export the combined data
if dataframes:
    # Combine all dataframes vertically
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # Export to a single CSV file
    # utf-8-sig encoding ensures Vietnamese characters (if any) display correctly in Excel
    output_filename = 'tong_hop_general_models.csv'
    combined_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
    
    print(f"\nMerge complete! The result is saved as: {output_filename}")
else:
    print("\nError: No files were merged. Please check your file paths.")
