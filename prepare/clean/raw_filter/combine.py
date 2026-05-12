import os
import argparse
import polars as pl

DEFAULT_ROOT_FILE = "raw_comments.parquet"
DEFAULT_NON_VI_TEMP = "non_vi_comments.csv"
DEFAULT_DEEP_VI_SLANG = "deep_vi_comments.parquet"
DEFAULT_OUT_FILE = "raw_filtered_comments.parquet"
DEFAULT_BATCH_SIZE = 500_000


def main():
    parser = argparse.ArgumentParser(description="Combine all to final raw-filtered comments.")
    parser.add_argument("--input", type=str, default=DEFAULT_ROOT_FILE, help="Input raw file")
    parser.add_argument("--non_vi_temp", type=str, default=DEFAULT_NON_VI_TEMP, help="Non-Vietnamese comments")
    parser.add_argument("--deep_vi_slang", type=str, default=DEFAULT_DEEP_VI_SLANG, help="Deep-filtered Vietnamese slang comments")
    parser.add_argument("--output", type=str, default=DEFAULT_OUT_FILE, help="Output file")
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size")

    args = parser.parse_args()

    raw_lf = pl.scan_parquet(args.input)
    nonvi_temp = pl.scan_csv(args.non_vi_temp)
    deep_vi_slang = pl.scan_parquet(args.deep_vi_slang)

    # Get comments_id to remove (nonvi_temp - deep_vi_slang)
    ids_to_remove = nonvi_temp.select("comment_id").join(
        deep_vi_slang.select("comment_id"),
        on="comment_id",
        how="anti"
    )

    # Remove comments
    final_lf = raw_lf.join(
        ids_to_remove,
        on="comment_id",
        how="anti"
    )

    # Save
    final_lf.sink_parquet(args.output, row_group_size=args.batch_size, engine="streaming")
    print(f"Filtered comments saved to {args.output}")

if __name__ == "__main__":
    main()
