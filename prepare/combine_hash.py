import polars as pl 
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import os

INPUT_FILE = "norm_filtered_comments.parquet"
OUTPUT_FILE = "lsh_bands.parquet" # Đổi tên file output cho chuẩn ngữ nghĩa
MID_FOLDER = "signatures"
BATCH_SIZE = 500_000
BANDS = 16
ROWS = 8

def get_year(in_lf: pl.LazyFrame) -> pl.LazyFrame:
    return in_lf.with_columns(
        [pl.col("created_at").str.to_datetime(strict=False).dt.year().alias("year")]
    )

def get_band_expressions():
    """Tạo trước danh sách các biểu thức (expressions) để gộp và hash các cột"""
    band_exprs = []
    for i in range(BANDS):
        # Lấy danh sách ROWS cột thuộc về band i
        cols = [pl.col(f"h_{i*ROWS + j}") for j in range(ROWS)]
        # Nối các số lại bằng dấu "_" rồi băm (hash) thành 1 giá trị u64 duy nhất
        band_hash = pl.concat_str(cols, separator="_").hash().alias(f"band_{i}")
        band_exprs.append(band_hash)
    return band_exprs

def combine_signatures(in_lf: pl.LazyFrame, total_rows: int, batch_size: int=BATCH_SIZE) -> None:
    num_chunks = (total_rows + batch_size - 1) // batch_size
    writer = None
    
    # Lấy các expression để xử lý LSH Banding
    band_exprs = get_band_expressions()

    for chunk_idx in range(num_chunks):
        npy_path = os.path.join(MID_FOLDER, f"chunk_{chunk_idx}.npy")
        sigs = np.load(npy_path)
        num_hash_cols = sigs.shape[1]
        
        h_columns = [f"h_{i}" for i in range(num_hash_cols)]

        # Tạo DataFrame tạm thời chứa 128 cột
        df_hashes = pl.DataFrame(sigs, schema=h_columns)
        
        # ---> BƯỚC QUAN TRỌNG: Nén 128 cột thành 16 cột Band <---
        # select() sẽ chạy đa luồng cực nhanh để băm dữ liệu
        df_bands = df_hashes.select(band_exprs)

        offset = chunk_idx * batch_size
        df_text = in_lf.select(["video_id", "year", "comment_id"]).slice(offset, batch_size).collect()

        # Nối DataFrame gốc với 16 cột Band (thay vì 128 cột như trước)
        df_combined = pl.concat([df_text, df_bands], how="horizontal")
        
        arrow_table = df_combined.to_arrow()
        if writer is None:
            writer = pq.ParquetWriter(OUTPUT_FILE, arrow_table.schema)
        writer.write_table(arrow_table)
        
        print(f"Đã xử lý xong chunk {chunk_idx + 1}/{num_chunks}")

    if writer is not None:
        writer.close()


def main():
    lf = pl.scan_parquet(INPUT_FILE)
    total_rows = lf.select(pl.len()).collect().item()

    lf = get_year(lf)

    combine_signatures(lf, total_rows)


if __name__ == "__main__":
    main()
