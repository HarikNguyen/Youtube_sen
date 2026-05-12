import polars as pl

df_pred = pl.scan_parquet("file_after_feed.parquet")

# The number of pseudo-labels that you want to get for each class
# Ex: get 3000 pseudo-labels for each class
K_SAMPLES = 3000 

df_balanced_pseudo = (
    df_pred
    .sort("confidence", descending=True)
    .group_by("labels")
    .head(K_SAMPLES)
    .collect()
)

# Print and Save
print("Distributed pseudo-labels after Top-K Sampling:")
print(df_balanced_pseudo["labels"].value_counts())
df_balanced_pseudo.write_parquet("balanced_pseudo_labels.parquet")
