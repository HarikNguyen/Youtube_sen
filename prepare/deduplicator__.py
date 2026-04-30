import polars as pl

BATCH_SIZE = 500_000

def find_all_duplicates(compact_lf):
    all_dup_pairs = []

    for i in range(16):
        print(f"Đang xử lý Band {i}...")
        
        band_dups = (
            compact_lf.select(["video_id", "year", "comment_id", f"b_{i}"])
            .group_by(["video_id", "year", f"b_{i}"])
            .agg(pl.col("comment_id").alias("ids"))
            .filter(pl.col("ids").list.len() > 1)
            .select("ids")
            .collect(engine="streaming") # Nạp kết quả nhỏ này vào RAM
        )
        
        # Với mỗi nhóm trùng, giữ lại ID đầu tiên, đánh dấu các ID còn lại là duplicate
        for row in band_dups.iter_rows():
            ids = row[0]
            canonical = ids[0]
            for duplicate in ids[1:]:
                all_dup_pairs.append(duplicate)

    return set(all_dup_pairs)

if __name__ == "__main__":
    compact_lf = pl.scan_parquet("lsh_compact.parquet")
    ids_to_remove = find_all_duplicates(compact_lf)

    df_to_remove = pl.DataFrame({"comment_id": list(ids_to_remove)})

    # Lọc file gốc bằng cách loại bỏ những ID nằm trong danh sách đen
    (
        pl.scan_parquet("norm_filtered_comments.parquet")
        .join(df_to_remove.lazy(), on="comment_id", how="anti")
        .sink_parquet("final_deduplicated_comments.parquet", engine="streaming", row_group_size=BATCH_SIZE)
    )

    print("Đã lọc và lưu file công!")
