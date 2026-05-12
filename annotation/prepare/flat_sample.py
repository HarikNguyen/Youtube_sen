import polars as pl

NUM_SAMPLE = 5033779
SAMPLE_FILE = f"sample_{NUM_SAMPLE}.parquet"
OUT_EXT = "parquet"
OUT_FILE = f"sampled_{NUM_SAMPLE}.{OUT_EXT}"

def flat(sample_lf: pl.LazyFrame) -> pl.LazyFrame:
    sample_lf = sample_lf.with_row_index("row_idx")
    return (
        sample_lf.with_columns(
            text = pl.when(
                pl.col("is_reply") & pl.col("parent_comment").is_not_null()
            )
            .then(
                pl.concat_str([
                    pl.lit("[TITLE] "), pl.col("title"),
                    pl.lit(" [CHANNEL] "), pl.col("channel"),
                    pl.lit(" [CATEGORY] "), pl.col("category"),
                    pl.lit(" [COMMENT] "), pl.col("parent_comment"),
                    pl.lit(" [REPLY] "), pl.col("comment"),
                    pl.lit(" [IN_YEAR] "), pl.col("year")
                ], separator="")
            )
            .when(~pl.col("is_reply"))
            .then(
                pl.concat_str([
                    pl.lit("[TITLE] "), pl.col("title"),
                    pl.lit(" [CHANNEL] "), pl.col("channel"),
                    pl.lit(" [CATEGORY] "), pl.col("category"),
                    pl.lit(" [COMMENT] "), pl.col("comment"),
                    pl.lit(" [IN_YEAR] "), pl.col("year")
                ], separator="")
            )
            .otherwise(None)
        )
        # Lọc bỏ những dòng không thỏa mãn điều kiện nào (nếu có)
        .filter(pl.col("text").is_not_null())
        # Chỉ chọn lại các cột cần thiết
        .select(["comment_id", "text",])
    )

def main():
    sample_lf = pl.scan_parquet(SAMPLE_FILE)
    sample_lf = flat(sample_lf)
    sample_lf.sink_parquet(OUT_FILE, row_group_size=500_000)

if __name__ == "__main__":
    main()
