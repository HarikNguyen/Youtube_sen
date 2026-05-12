import argparse
import polars as pl

DEFAULT_INPUT_FILE = "deduplicated_comments.parquet"
DEFAULT_OUTPUT_FILE = "q3_filtered_comments.parquet"
DEFAULT_BATCH_SIZE = 500_000

def filter_Q3(lf: pl.LazyFrame) -> pl.LazyFrame:
    # Get token_count and year
    lf = lf.with_columns(
        token_count = pl.col("comment").str.count_matches(r"\s+").fill_null(0) + 1,
        year = pl.col("created_at").str.to_datetime(strict=False).dt.year()
    )
    
    # Filter the token count to be less than the 75th percentile of the token count within each group
    lf = lf.filter(
        pl.col("token_count") < pl.col("token_count").quantile(0.75).over(["video_id", "year"])
    )
    
    # Remove token_count
    return lf.drop(["token_count"])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--input_file", type=str, default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output_file", type=str, default=DEFAULT_OUTPUT_FILE)
    args = parser.parse_args()

    # Load data
    lf = pl.scan_parquet(args.input_file)

    # Filter Q3 and save
    lf = filter_Q3(lf)
    lf.sink_parquet(args.output_file, engine="streaming", row_group_size=args.batch_size)

if __name__ == "__main__":
    main()
