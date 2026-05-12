import polars as pl

FILES = ["ft_seed_+uit.parquet", "ft_90k.parquet", "ft_848k.parquet", "final_2M.parquet"]
RAW_FILE = "final_prepared_comments.parquet"
OUT_FILE = "final_labeld_comments.parquet"
BATCH_SIZE = 500_000

def main():

    # get all comment_id and label
    labeled_lfs = []
    keep_cols = ["comment_id", "text", "labels"]
    for f in FILES:
        labeled_lfs.append(pl.scan_parquet(f).select(keep_cols))

    labeled_lf = pl.concat(labeled_lfs)

    # load raw 
    raw_lf = pl.scan_parquet(RAW_FILE)

    # filter and join label based on comment_id
    final_lf = raw_lf.join(
        labeled_lf,
        on="comment_id",
        how="inner"
    )

    final_lf.sink_parquet(OUT_FILE, row_group_size=BATCH_SIZE, engine="streaming")

    # stats
    totals = final_lf.select([
        pl.len().alias("total_rows"),
        pl.col("labels").n_unique().alias("total_labels")
    ]).collect()

    total_rows = totals["total_rows"][0]
    total_labels = totals["total_labels"][0]

    print(f"Total rows: {total_rows}")
    print(f"Total unique labels: {total_labels}")
    stats_df = (
        final_lf.group_by("labels")
        .agg([
            pl.len().alias("count"),
        ])
        .with_columns([
            (pl.col("count") / total_rows * 100).round(2).alias("percent")
        ])
        .sort("count", descending=True)
        .collect()
    )
    for row in stats_df.iter_rows():
        print(row)
 
if __name__ == "__main__":
    main()

