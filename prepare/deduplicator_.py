import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import polars as pl
import pyarrow.parquet as pq
from datasketch import MinHash

INPUT_FILE = "norm_filtered_comments.parquet"
OUTPUT_FILE = "deduplicated_comments.parquet"
MID_FOLDER = "signatures"
MID_FILE = "dedup_temp.parquet"
BATCH_SIZE = 500_000
NUM_HASHES = 128
NUM_CORES = max(1, multiprocessing.cpu_count() - 1)

def get_year(in_lf: pl.LazyFrame) -> pl.LazyFrame:
    return in_lf.with_columns(
        [pl.col("created_at").str.to_datetime(strict=False).dt.year().alias("year")]
    )

def combine_signatures(in_lf: pl.LazyFrame, total_rows: int, batch_size: int=BATCH_SIZE) -> None:
    for chunk_idx in range(num_chunks):
        npy_path = os.path.join(MID_FOLDER, f"chunk_{chunk_idx}.npy")
        if os.path.exists(npy_path):
            sigs = np.load(npy_path)
            all_hashes.extend([compress_signature(sig) for sig in sigs])
    

def extract_unique_ids(in_lf: pl.LazyFrame, total_rows: int) -> None:

    hashed_lf = pl.scan_parquet(MID_FILE)
    unique_ids = hashed_lf.groupby("comment_hash").agg(
        [
            pl.col("comment_id").first().alias("comment_id"),
            pl.col("video_id").first().alias("video_id"),
            pl.col("year").first().alias("year"),
        ]
    )

    print(f"Writing unique comments to {MID_FILE2}")
    unique_ids.write_parquet(MID_FILE2)

def deduplicate_by_video_with_minhash(in_lf: pl.LazyFrame) -> pl.LazyFrame:
    return in_lf.groupby(["video_id", "year"]).agg(
        [
            pl.col("comment_id").first().alias("comment_id"),
            pl.col("video_id").first().alias("video_id"),
            pl.col("year").first().alias("year"),
        ]
    )
    

def main():
    lf = pl.scan_parquet(INPUT_FILE)
    total_rows = lf.select(pl.len()).collect().item()

    # Add year column (extract from created_at)
    lf = get_year(lf)

    extract_unique_ids(lf, total_rows)

    deduped_lf = deduplicate_by_video_with_minhash(pl.scan_parquet(MID_FILE2))

    print(f"Writing deduplicated comments to {OUTPUT_FILE}")
    deduped_lf.write_parquet(OUTPUT_FILE)


if __name__ == "__main__":
    main()
