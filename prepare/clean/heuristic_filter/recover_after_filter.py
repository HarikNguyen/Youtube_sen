import argparse
import polars as pl

DEFAULT_INPUT_FILE = "q3_filtered_comments.parquet"
DEAFULT_SUPPLY_FILE = "norm_filtered_comments.parquet"
DEFAULT_OUTPUT_FILE = "final_prepared_comments.parquet"
DEFAULT_BATCH_SIZE = 500_000

def get_year(in_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Get year from created_at column"""
    return in_lf.with_columns(
        [pl.col("created_at").str.to_datetime(strict=False).dt.year().alias("year")]
    )

def find_missing(lf: pl.LazyFrame) -> pl.DataFrame:
    unique_parents_lf = (
        lf.filter(
            (pl.col("parent_id") != "root") & 
            (pl.col("parent_id").is_not_null())
        )
        .select("parent_id")
        .unique()
    )

    existing_comments_lf = (
        lf.select("comment_id")
        .unique()
    )

    missing_parents_df = (
        unique_parents_lf.join(
            existing_comments_lf, 
            left_on="parent_id", 
            right_on="comment_id", 
            how="anti"
        )
        .collect(engine="streaming")
    )

    return missing_parents_df

def get_supply(sup_lf: pl.LazyFrame, missing_parents_df: pl.DataFrame) -> pl.LazyFrame:
    if missing_parents_df.height == 0:
        return

    return (
        sup_lf
        .join(
            missing_parents_df.lazy(), 
            left_on="comment_id", 
            right_on="parent_id", 
            how="semi" 
        )
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--input_file", type=str, default=DEFAULT_INPUT_FILE)
    parser.add_argument("--supply_file", type=str, default=DEAFULT_SUPPLY_FILE)
    parser.add_argument("--output_file", type=str, default=DEFAULT_OUTPUT_FILE)
    args = parser.parse_args()

    # load data
    lf = pl.scan_parquet(args.input_file)
    if "year" not in lf.collect_schema().names():
        lf = get_year(lf)

    # get supply
    missing_parents_df = find_missing(lf)
    sup_lf = pl.scan_parquet(args.supply_file)
    supply_lf = get_supply(sup_lf, missing_parents_df)
    if "year" not in supply_lf.collect_schema().names():
        supply_lf = get_year(supply_lf)

    (
        pl.concat([lf, supply_lf], how="vertical")
        .sink_parquet(
            args.output_file,
            engine="streaming", 
            row_group_size=args.batch_size
        )
    )
    
if __name__ == "__main__":
    main()
