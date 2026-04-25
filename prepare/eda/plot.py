import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Load Data
metadata = pd.read_parquet("raw_videos.parquet")
comments = pd.read_parquet("raw_comments.parquet")

# 2. Optimized Mapping (Preventing crashes with 10M rows)
category_map = metadata.set_index("video_id")["category"]
type_map = metadata.set_index("video_id")["video_type"]

# 3. Data Aggregation
video_category_counts = metadata["category"].value_counts()
comment_category_counts = comments["video_id"].map(category_map).value_counts()
comment_type_counts = comments["video_id"].map(type_map).value_counts()
top_10_commented = comments["video_id"].value_counts().head(10)

# 4. Visualization Config
sns.set_theme(style="whitegrid")
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle(
    "YouTube Video & Comments Analytics Dashboard", fontsize=20, fontweight="bold"
)

# --- PLOT 1: Number of Videos by Category ---
sns.barplot(
    x=video_category_counts.values,
    y=video_category_counts.index,
    ax=axes[0, 0],
    palette="viridis",
)
axes[0, 0].set_title("Video Distribution by Category", fontsize=14)
axes[0, 0].set_xlabel("Total Videos")
axes[0, 0].set_ylabel("Category")

# --- PLOT 2: Number of Comments by Category ---
sns.barplot(
    x=comment_category_counts.values,
    y=comment_category_counts.index,
    ax=axes[0, 1],
    palette="magma",
)
axes[0, 1].set_title("Comment Volume by Category", fontsize=14)
axes[0, 1].set_xlabel("Total Comments")
axes[0, 1].set_ylabel("Category")

# --- PLOT 3: Comment Distribution by Video Type (Pie Chart) ---
axes[1, 0].pie(
    comment_type_counts,
    labels=comment_type_counts.index,
    autopct="%1.1f%%",
    startangle=140,
    colors=sns.color_palette("pastel"),
)
axes[1, 0].set_title("Comment Share by Video Type", fontsize=14)

# --- PLOT 4: Top 10 Most Commented Videos ---
sns.barplot(
    x=top_10_commented.values,
    y=top_10_commented.index.astype(str),
    ax=axes[1, 1],
    palette="rocket",
)
axes[1, 1].set_title("Top 10 Most Commented Videos", fontsize=14)
axes[1, 1].set_xlabel("Number of Comments")
axes[1, 1].set_ylabel("Video ID")

# 5. Final Adjustment
plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.show()
