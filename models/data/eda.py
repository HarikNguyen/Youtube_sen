import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

RAW_CMT = "final_labeld_comments.parquet"
RAW_VID = "raw_videos.parquet"


def process_data():
    cmt_lf = pl.scan_parquet(RAW_CMT)
    vid_lf = pl.scan_parquet(RAW_VID)

    # Get list of video_ids that have comments
    valid_video_ids = cmt_lf.select("video_id").unique()

    # Filter: Video & Comment. Only keep rows that are in valid_video_ids and have comments
    vid_filtered = vid_lf.join(valid_video_ids, on="video_id", how="inner").collect()
    cmt_filtered = cmt_lf.join(
        vid_lf.select("video_id"), on="video_id", how="inner"
    ).collect()

    print(f"--- Shape & Columns ---")
    print(f"Video PF: {vid_filtered.shape} | Columns: {vid_filtered.columns}")
    print(f"Comment PF: {cmt_filtered.shape} | Columns: {cmt_filtered.columns}\n")

    # Stats cmt_counts: min, max, mean & violin plot
    cmt_counts = (
        cmt_filtered.group_by("video_id")
        .agg(pl.count("video_id").alias("comment_count"))
        .sort("comment_count")
    )

    stats = cmt_counts.select(
        min=pl.col("comment_count").min(),
        max=pl.col("comment_count").max(),
        mean=pl.col("comment_count").mean(),
    )
    print(f"--- comment/video statistics ---")
    print(stats)

    plt.figure(figsize=(10, 6))
    sns.violinplot(y=cmt_counts["comment_count"], inner="point", color="skyblue")
    plt.title("Distribution of Comment Counts per Video (Violin Chart)")
    plt.ylabel("Number of Comments")
    plt.savefig("comment_dist_violin.png")
    plt.close()

    # 3. Thống kê theo video_type
    # Video type: short, long
    joined_df = cmt_filtered.join(
        vid_filtered.select(["video_id", "video_type"]), on="video_id"
    )

    vid_type_stats = (
        vid_filtered.group_by("video_type")
        .agg(num_videos=pl.count("video_id"))
        .join(
            joined_df.group_by("video_type").agg(num_comments=pl.count()),
            on="video_type",
        )
    )
    print(f"\n--- Video Type stats ---")
    print(vid_type_stats)

    # Plot the label distribution (28 labels)
    label_order = [
        "amusement",
        "excitement",
        "joy",
        "love",
        "desire",
        "optimism",
        "caring",
        "pride",
        "admiration",
        "gratitude",
        "relief",
        "approval",
        "realization",
        "surprise",
        "curiosity",
        "confusion",
        "fear",
        "nervousness",
        "remorse",
        "embarrassment",
        "disappointment",
        "sadness",
        "grief",
        "disgust",
        "anger",
        "annoyance",
        "disapproval",
        "neutral",
    ]

    sentiment_groups = {
        "Positive": label_order[0:12],
        "Ambiguous": label_order[12:16],
        "Negative": label_order[16:27],
        "Neutral": [label_order[27]],
    }

    label_counts = cmt_filtered.group_by("labels").count().to_pandas()
    label_counts = (
        label_counts.set_index("labels").reindex(label_order).reset_index().fillna(0)
    )

    colors = []
    for lbl in label_counts["labels"]:
        if lbl in sentiment_groups["Positive"]:
            colors.append("#4CAF50")  # Green
        elif lbl in sentiment_groups["Ambiguous"]:
            colors.append("#FFC107")  # Yellow/Gold
        elif lbl in sentiment_groups["Negative"]:
            colors.append("#FF8A65")  # Coral/Orange
        else:
            colors.append("#9E9E9E")  # Grey

    plt.figure(figsize=(15, 7))
    bars = plt.bar(label_counts["labels"], label_counts["len"], color=colors)
    plt.xticks(rotation=90)
    plt.title("Sample distribution of each label")
    plt.ylabel("Number of samples")

    # Add value labels in the bars
    for bar in bars:
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            yval + 5,
            int(yval),
            ha="center",
            va="bottom",
            fontsize=8,
        )

    plt.tight_layout()
    plt.savefig("label_distribution.png")
    plt.close()

    # --- Token statistics (Q1, Q3, and IQR) ---
    cmt_filtered = cmt_filtered.with_columns(
        pl.col("text").str.split(" ").list.len().alias("token_count")
    )
    stats = cmt_filtered.select(
        min=pl.col("token_count").min(),
        q1=pl.col("token_count").quantile(0.25),
        mean=pl.col("token_count").mean(),
        q3=pl.col("token_count").quantile(0.75),
        max=pl.col("token_count").max(),
        count_lq3=pl.col("token_count")
        .filter((pl.col("token_count") <= pl.col("token_count").quantile(0.75)))
        .len(),
    )
    print(f"--- Token statistics ---")
    print(stats)

    # Create char count column
    cmt_filtered = cmt_filtered.with_columns(
        pl.col("text").str.len_chars().alias("char_count")
    )
    stats = cmt_filtered.select(
        min=pl.col("char_count").min(),
        q1=pl.col("char_count").quantile(0.25),
        mean=pl.col("char_count").mean(),
        q3=pl.col("char_count").quantile(0.75),
        max=pl.col("char_count").max(),
        count_lq3=pl.col("char_count")
        .filter((pl.col("char_count") <= pl.col("char_count").quantile(0.75)))
        .len(),
        count_512=pl.col("char_count")
        .filter((pl.col("char_count") <= pl.col("char_count").quantile(0.75)))
        .len(),
    )

    print(f"--- Char statistics ---")
    print(stats)


if __name__ == "__main__":
    process_data()
