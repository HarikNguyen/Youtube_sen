import argparse
import math
import polars as pl
import pandas as pd
import matplotlib.pyplot as plt

DEFAULT_K = 2
DEFAULT_INPUT = "final_labeld_comments.parquet"
DEFAULT_INPUTMETA = "raw_videos.parquet"
DEFAULT_OUTPUT = "unsampled_comments.parquet"


############################################################################################
### Utils
############################################################################################
def show_label_dist(
    lf: pl.LazyFrame, show_plot: bool = False, plot_name: str = "label_distribution.png"
) -> pl.DataFrame:
    label_counts = lf.group_by("labels").len().collect().to_pandas()

    if show_plot:
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

        label_counts = (
            label_counts.set_index("labels")
            .reindex(label_order)
            .reset_index()
            .fillna(0)
        )
        label_counts["labels"] = pd.Categorical(
            label_counts["labels"], categories=label_order, ordered=True
        )
        sentiment_groups = {
            "Positive": label_order[0:12],
            "Ambiguous": label_order[12:16],
            "Negative": label_order[16:27],
            "Neutral": [label_order[27]],
        }

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
        plt.xticks(rotation=45, ha="right", rotation_mode="anchor")
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
        plt.savefig(plot_name)
        plt.close()

    return label_counts


def sample_by_label(
    label_lf: pl.LazyFrame, label_name: str, num_to_sample: int
) -> pl.LazyFrame:
    # Sample via comment_id
    avail_ids = label_lf.select("comment_id").collect().to_series()
    sampled_ids = avail_ids.sample(
        n=num_to_sample, with_replacement=False, shuffle=True, seed=42
    )
    sampled_ids_df = sampled_ids.to_frame().lazy()

    label_lf = label_lf.join(sampled_ids_df, on="comment_id", how="inner")
    return label_lf


def get_year(in_lf: pl.LazyFrame) -> pl.LazyFrame:
    if "year" not in in_lf.collect_schema().names():
        return in_lf.with_columns(
            [pl.col("created_at").str.to_datetime(strict=False).dt.year().alias("year")]
        )
    else:
        return in_lf


def correct_text(cmt_lf: pl.LazyFrame, vid_lf: pl.LazyFrame) -> pl.LazyFrame:
    vid_meta = vid_lf.select(["video_id", "title", "channel", "category"])
    cmt_lf = cmt_lf.join(vid_meta, on="video_id", how="left")

    # Get parent comments for preparing the child comments.
    parents_lf = cmt_lf.filter(
        (pl.col("is_reply") == False)
        | (pl.col("is_reply") == "False")
        | (pl.col("is_reply") == "0")
    ).select(
        pl.col("comment_id").alias("parent_id_join"),
        pl.col("comment").alias("parent_comment"),
    )
    # Finish join
    cmt_lf = cmt_lf.join(
        parents_lf, left_on="parent_id", right_on="parent_id_join", how="left"
    )

    format_parent = pl.concat_str(
        [
            pl.lit("[TITLE] "),
            pl.col("title").fill_null(""),
            pl.lit(" [CHANNEL] "),
            pl.col("channel").fill_null(""),
            pl.lit(" [CATEGORY] "),
            pl.col("category").fill_null(""),
            pl.lit(" [COMMENT] "),
            pl.col("comment").fill_null(""),
            pl.lit(" [IN_YEAR] "),
            pl.col("year").cast(pl.Utf8).fill_null(""),
        ]
    )

    format_reply = pl.concat_str(
        [
            pl.lit("[TITLE] "),
            pl.col("title").fill_null(""),
            pl.lit(" [CHANNEL] "),
            pl.col("channel").fill_null(""),
            pl.lit(" [CATEGORY] "),
            pl.col("category").fill_null(""),
            pl.lit(" [COMMENT] "),
            pl.col("parent_comment").fill_null(""),
            pl.lit(" [REPLY] "),
            pl.col("comment").fill_null(""),
            pl.lit(" [IN_YEAR] "),
            pl.col("year").cast(pl.Utf8).fill_null(""),
        ]
    )

    cmt_lf = cmt_lf.with_columns(
        text=pl.when(
            (pl.col("is_reply") == True)
            | (pl.col("is_reply") == "True")
            | (pl.col("is_reply") == "1")
        )
        .then(format_reply)
        .otherwise(format_parent)
    ).drop("parent_comment")

    return cmt_lf


############################################################################################
### Main
############################################################################################


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input", type=str, default=DEFAULT_INPUT, help="Path to the input CSV file."
    )
    parser.add_argument(
        "--inputmeta",
        type=str,
        default=DEFAULT_INPUTMETA,
        help="Path to the input metadata CSV file.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT,
        help="Path to the output CSV file.",
    )
    parser.add_argument(
        "--labels",
        type=str,
        action="append",
        required=True,
        help="List of the labels to unsample.",
    )
    parser.add_argument(
        "--K",
        type=float,
        required=True,
        help="Factor (K) by which to unsample the dataset.",
    )
    parser.add_argument(
        "--unwrite",
        action="store_true",
        default=False,
        help="Unwrite the output file.",
    )

    args = parser.parse_args()

    # Scan input file
    cmt_lf = pl.scan_parquet(args.input)
    vid_lf = pl.scan_parquet(args.inputmeta)

    # Preprocess
    cmt_lf = get_year(cmt_lf)
    # Correct the text
    cmt_lf = correct_text(cmt_lf, vid_lf)

    # Get list of video_ids that have comments
    valid_video_ids = cmt_lf.select("video_id").unique()

    # Filter: Video & Comment. Only keep rows that are in valid_video_ids and have comments
    vid_filtered = vid_lf.join(valid_video_ids, on="video_id", how="inner")
    cmt_filtered = cmt_lf.join(vid_lf.select("video_id"), on="video_id", how="inner")

    # Get label distribution
    label_counts = show_label_dist(cmt_filtered)
    # Calc the median of the label distribution
    median_count = label_counts["len"].median()

    sampled_lfs = []
    for label in args.labels:
        # Filter the LazyFrame for the current label
        label_lf = cmt_filtered.filter(pl.col("labels") == label)
        n_o_label = label_lf.select(pl.len()).collect().item()
        # Calc the number of samples to keep for each labels
        num_to_draw = int(math.sqrt(median_count * n_o_label)) * args.K
        print(
            f"Median count: {median_count}, New number of samples per label: {num_to_draw}"
        )

        # Sample new_num
        label_sample_lf = sample_by_label(label_lf, label, num_to_draw)
        sampled_lfs.append(label_sample_lf)

    # Combine sampled_lfs into one LazyFrame
    other_lf = cmt_filtered.filter(~pl.col("labels").is_in(args.labels))
    final_lf = pl.concat(sampled_lfs + [other_lf])

    # Select columns to output
    final_lf = (
        final_lf.with_columns(pl.lit(1).alias("random_index"))
        .with_columns(
            pl.col("random_index").sample(fraction=1.0, shuffle=True, seed=42)
        )
        .sort("random_index")
        .drop("random_index")
        .select(["text", "labels"])
    )

    # total count after sampling
    total = final_lf.select(pl.len()).collect().item()
    print(f"Total count after sampling: {total}")
    label_dist = show_label_dist(
        final_lf, show_plot=True, plot_name="label_dist_after_unsampling.png"
    )

    # Write to output filter
    if not args.unwrite:

        final_lf.sink_parquet(args.output, row_group_size=500_000)


if __name__ == "__main__":
    main()
