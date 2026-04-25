import polars as pl

# Đọc dữ liệu
pl1 = pl.scan_parquet("raw_comments.parquet")
pl2 = pl.read_csv("non_vi_comments_glotlid.csv")
pl3 = pl.read_csv("test_02.csv")

# Chuyển về lazy
lazy_pl2 = pl2.lazy()
lazy_pl3 = pl3.lazy()

# Logic lọc: Loại bỏ khỏi pl2 những id có trong pl3
exclude_ids = lazy_pl2.join(lazy_pl3, on="comment_id", how="anti")

# Logic chính: Loại bỏ khỏi pl1 những id nằm trong danh sách exclude_ids
result = pl1.join(exclude_ids, on="comment_id", how="anti")

# Ghi dữ liệu xuống file một cách tối ưu bộ nhớ (Streaming)
result.sink_parquet(
    "raw_filtered_comments.parquet",
    compression="snappy",  # Bạn có thể chọn 'zstd' nếu muốn file nhỏ hơn nữa
    row_group_size=500_000,  # Tương đương "batch size" khi ghi file
)

print("Đã lọc và lưu file thành công!")
