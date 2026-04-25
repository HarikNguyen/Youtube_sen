import polars as pl

df2 = pl.read_csv("non_vi_comments_glotlid.csv").select("comment_id")

# Sử dụng Anti-join với cơ chế sink_parquet
# Polars sẽ tự chia dữ liệu thành các cụm (chunks) để xử lý mà không làm tràn RAM
(
    pl.scan_parquet("raw_comments.parquet")
    .select(["comment_id", "comment"])
    .join(df2.lazy(), on="comment_id", how="anti")
    .sink_parquet("filtered_comments.parquet")
)

print("Đã lọc và xuất file thành công!")
