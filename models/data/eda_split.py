import polars as pl
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

FILES = ["train.parquet", "val.parquet", "test.parquet"]


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


def plot_dist():
    for file in FILES:
        pf = pl.scan_parquet(file)
        name = file.split(".")[0]
        label_counts = show_label_dist(
            pf, show_plot=True, plot_name=f"{name}_label_dist.png"
        )


if __name__ == "__main__":
    plot_dist()
