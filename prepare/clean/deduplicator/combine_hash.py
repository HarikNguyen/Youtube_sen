import argparse
import polars as pl 
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import os

DEFAULT_INPUT_FILE = "norm_filtered_comments.parquet"
DEFAULT_OUTPUT_FILE = "lsh_bands.parquet"
DEFAULT_MID_FOLDER = "signatures"
DEFAULT_BATCH_SIZE = 500_000
BANDS= 16
ROWS = 8

def get_year(in_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Get year from created_at column"""
    return in_lf.with_columns(
        [pl.col("created_at").str.to_datetime(strict=False).dt.year().alias("year")]
    )

def get_band_expressions():
    """Create expressions to group and hash columns"""
    band_exprs = []
    for i in range(BANDS):
        # Get list of ROWS columns for band i
        cols = [pl.col(f"h_{i*ROWS + j}") for j in range(ROWS)]
        # Concatenate list of columns by "_". And then hash to get a unique value
        band_hash = pl.concat_str(cols, separator="_").hash().alias(f"band_{i}")
        band_exprs.append(band_hash)
    return band_exprs

def combine_signatures(in_lf: pl.LazyFrame, total_rows: int, batch_size: int=DEFAULT_BATCH_SIZE, mid_folder: str=DEFAULT_MID_FOLDER, output_file: str=DEFAULT_OUTPUT_FILE) -> None:
    num_chunks = (total_rows + batch_size - 1) // batch_size
    writer = None
    
    band_exprs = get_band_expressions()

    for chunk_idx in range(num_chunks):
        npy_path = os.path.join(mid_folder, f"chunk_{chunk_idx}.npy")
        sigs = np.load(npy_path)
        num_hash_cols = sigs.shape[1]
        
        h_columns = [f"h_{i}" for i in range(num_hash_cols)]

        df_hashes = pl.DataFrame(sigs, schema=h_columns)
        
        # Compress 128 columns into 16 band columns
        df_bands = df_hashes.select(band_exprs)

        offset = chunk_idx * batch_size
        df_text = in_lf.select(["video_id", "year", "comment_id"]).slice(offset, batch_size).collect()

        df_combined = pl.concat([df_text, df_bands], how="horizontal")
        
        arrow_table = df_combined.to_arrow()
        if writer is None:
            writer = pq.ParquetWriter(output_file, arrow_table.schema)
        writer.write_table(arrow_table)
        
        print(f"Chunk {chunk_idx+1}/{num_chunks} written to {output_file}")

    if writer is not None:
        writer.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", type=str, default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output_file", type=str, default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--mid_folder", type=str, default=DEFAULT_MID_FOLDER)
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()

    # Load inf
    lf = pl.scan_parquet(args.input_file)
    total_rows = lf.select(pl.len()).collect().item()
    lf = get_year(lf)

    # Combine hash
    combine_signatures(lf, total_rows, args.batch_size, args.mid_folder, args.output_file)


if __name__ == "__main__":
    main()
