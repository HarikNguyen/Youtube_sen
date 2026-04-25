import pandas as pd

# pandas config
pd.set_option("display.max_columns", None)

metadata = pd.read_parquet("raw_videos.parquet")
comments = pd.read_parquet("raw_comments.parquet")

print(metadata.head())
print(comments.head())

print("================================")
print(metadata.shape)
print(comments.shape)

print("================================")
# stats on metadata
# how many videos in each category
print("Number of videos in each category:")
print(metadata["category"].value_counts())

# stats on comments
# how many comments for each video (top 10)
print("Top 10 videos with most comments:")
print(comments["video_id"].value_counts().head(10))

# how many comments in each category
print("Number of comments in each category:")
category_map = metadata.set_index("video_id")["category"]
type_map = metadata.set_index("video_id")["video_type"]
print(comments["video_id"].map(category_map).value_counts())

# how many comments in each video_type
print("Number of comments in each video type:")
print(comments["video_id"].map(type_map).value_counts())
