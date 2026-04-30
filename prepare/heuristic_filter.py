import polars as pl

INPUT_FILE = "final_deduplicated_comments_supplemented.parquet"
OUTPUT_FILE = "deep_filtered_comments.parquet"
BATCH_SIZE = 500_000

def filter_Q3(lf: pl.LazyFrame) -> pl.LazyFrame:
    # 1. Tạo cột token_count và cột year tạm thời
    lf = lf.with_columns(
        token_count = pl.col("comment").str.count_matches(r"\s+").fill_null(0) + 1,
        
        # CÁCH 1: Nếu created_at đang là kiểu Date/Datetime
        year = pl.col("created_at").str.to_datetime(strict=False).dt.year()
        
        # CÁCH 2: Nếu created_at đang là kiểu String (vd: "2023-10-25T...")
        # Hãy comment Cách 1 lại và bỏ comment Cách 2 dưới đây:
        # year = pl.col("created_at").str.slice(0, 4)
    )
    
    # 2. Lọc token_count < Q3 của từng nhóm (video_id, year)
    lf = lf.filter(
        pl.col("token_count") < pl.col("token_count").quantile(0.75).over(["video_id", "year"])
    )
    
    # 3. Xóa các cột tạm để giữ nguyên cấu trúc dữ liệu ban đầu
    return lf.drop(["token_count", "year"])

def main():
    lf = pl.scan_parquet(INPUT_FILE)
    lf = filter_Q3(lf)
    lf.sink_parquet(OUTPUT_FILE, engine="streaming", row_group_size=BATCH_SIZE)

if __name__ == "__main__":
    main()
