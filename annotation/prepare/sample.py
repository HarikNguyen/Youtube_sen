import polars as pl
import numpy as np

INPUT_FILE = "final_prepared_comments.parquet"
VIDEO_FILE = "raw_videos.parquet"
OUTPUT_FILE = "sample_{}.parquet"

# sampled files (All sampled file after each fine-tune loop like ft_seed_+uit.parquet, ft_90k.parquet, ... etc)
OLD_SAMPLE = ["ft_848k.parquet","ft_173k.parquet", "ft_seed_+uit.parquet", "ft_90k.parquet"]

# the size of sample in next fine-tune loop (like 10000, 20000, ... etc). If None, all data will be sampled
SAMPLE_SIZE = None

def get_year(in_lf: pl.LazyFrame) -> pl.LazyFrame:
    if "year" not in in_lf.collect_schema().names():
        return in_lf.with_columns(
            [pl.col("created_at").str.to_datetime(strict=False).dt.year().alias("year")]
        )
    else:
        return in_lf


def sample(lf: pl.LazyFrame, total_sz, sample_sz: int) -> pl.LazyFrame:
    random_indices = np.random.choice(total_sz, size=sample_sz, replace=False)
    df_sampled = (
        lf.with_row_index("row_idx")
        .filter(pl.col("row_idx").is_in(random_indices))
        .drop("row_idx")
    )
    return df_sampled


def find_missing(lf: pl.LazyFrame) -> pl.DataFrame:

    unique_parents_lf = (
        lf.filter((pl.col("parent_id") != "root") & (pl.col("parent_id").is_not_null()))
        .select("parent_id")
        .unique()
    )

    existing_comments_lf = lf.select("comment_id").unique()

    missing_parents_df = unique_parents_lf.join(
        existing_comments_lf, left_on="parent_id", right_on="comment_id", how="anti"
    ).collect(engine="streaming")

    return missing_parents_df


def get_supply(sup_lf: pl.LazyFrame, missing_parents_df: pl.DataFrame) -> pl.LazyFrame:
    if missing_parents_df.height == 0:
        return

    return sup_lf.join(
        missing_parents_df.lazy(),
        left_on="comment_id",
        right_on="parent_id",
        how="semi",
    )


def add_parent_cmt(sample_lf: pl.LazyFrame, source_lf: pl.LazyFrame) -> pl.LazyFrame:
    missing_parents_df = find_missing(sample_lf)
    sup_lf = get_supply(source_lf, missing_parents_df)
    return pl.concat([sample_lf, sup_lf], how="vertical")


def expand_video(video_lf: pl.LazyFrame, sample_lf: pl.LazyFrame) -> pl.LazyFrame:
    video_lf = video_lf.select(
        ["video_id", "title", "channel", "category", "video_type"]
    )

    # expand sampled_lf with video_lf
    expand_lf = sample_lf.join(video_lf, on="video_id", how="left")
    return expand_lf


def reconstruct(lf: pl.LazyFrame) -> pl.LazyFrame:
    parent_lookup = lf.select(["comment_id", "comment"]).rename(
        {"comment": "parent_comment"}
    )

    return lf.join(
        parent_lookup, left_on="parent_id", right_on="comment_id", how="left"
    )


def main():
    lf = pl.scan_parquet(INPUT_FILE)

    # ignore sampled cmt_id
    if len(OLD_SAMPLE) > 0:
        sampled_ids = (
            pl.concat(
                [pl.scan_parquet(file).select("comment_id") for file in OLD_SAMPLE]
            )
            .unique()
            .collect()["comment_id"]  # Lấy cột comment_id ra dưới dạng Series
        )
    else:
        sampled_ids = []

    lf = lf.filter(~pl.col("comment_id").is_in(sampled_ids))
    total_rows = lf.select(pl.len()).collect().item()

    if SAMPLE_SIZE is None:
        sample_size = total_rows
        sample_lf = lf
    else:
        sample_size = SAMPLE_SIZE
        sample_lf = sample(lf, total_rows, sample_size)

    sample_lf = add_parent_cmt(sample_lf, lf)

    video_lf = pl.scan_parquet(VIDEO_FILE)
    sample_lf = expand_video(video_lf, sample_lf)

    sample_lf = reconstruct(sample_lf)
    sample_lf = get_year(sample_lf)
    sample_df = sample_lf.collect(engine="streaming")

    sample_df.write_parquet(OUTPUT_FILE.format(sample_size))


if __name__ == "__main__":
    main()
