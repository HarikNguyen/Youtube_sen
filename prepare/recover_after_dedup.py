import polars as pl

INPUT_FILE = "deep_filtered_comments.parquet"
SUPPLY_FILE = "norm_filtered_comments.parquet"
OUTPUT_FILE = "final_prepared_comments.parquet"
BATCH_SIZE = 500_000

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
    lf = pl.scan_parquet(INPUT_FILE)
    missing_parents_df = find_missing(lf)
    sup_lf = pl.scan_parquet(SUPPLY_FILE)
    supply_lf = get_supply(sup_lf, missing_parents_df)
    
    (
        pl.concat([lf, supply_lf], how="vertical")
        .sink_parquet(
            OUTPUT_FILE,
            engine="streaming", 
            row_group_size=BATCH_SIZE
        )
    )
    
if __name__ == "__main__":
    main()
