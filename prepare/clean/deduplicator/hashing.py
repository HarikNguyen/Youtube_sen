import os
import shutil
import multiprocessing as mp
import argparse
import numpy as np
import pyarrow.parquet as pq
from tqdm import tqdm
from datasketch import MinHash, LeanMinHash


DEFAULT_INPUT_FILE = "norm_filtered_comments.parquet"
DEFAULT_OUTPUT_DIR = "signatures"
DEFAULT_TEXT_COLUMN = "comment"
DEFAULT_NUM_PERM = 128            
DEFAULT_CHUNK_SIZE = 500_000
DEFAULT_NUM_CORES = 3             

def get_minhash(text, num_perm=DEFAULT_NUM_PERM):
    """Hashing for each document comment"""
    # Check null comment
    if not text or not isinstance(text, str):
        text = ""
    
    tokens = text.strip().lower().split()
    m_obj = MinHash(num_perm=num_perm)
    for word in tokens:
        m_obj.update(word.encode('utf8'))
        
    # Convert to LeanMinHash to optimize space
    return LeanMinHash(m_obj).hashvalues

def process_chunk(texts, chunk_id, out_dir=DEFAULT_OUTPUT_DIR, num_cores=DEFAULT_NUM_CORES):
    """Multiprocessing function to process a chunk of data"""
    with mp.Pool(num_cores) as pool:
        signatures = list(pool.map(get_minhash, texts))
    
    file_path = os.path.join(out_dir, f"chunk_{chunk_id}.npy")
    np.save(file_path, np.array(signatures, dtype=np.uint64))
    return file_path

def hashing(inf, total_rows, text_column=DEFAULT_TEXT_COLUMN, out_dir=DEFAULT_OUTPUT_DIR, num_perm=DEFAULT_NUM_PERM, chunk_size=DEFAULT_CHUNK_SIZE, num_cores=DEFAULT_NUM_CORES):

    chunk_count = 0
    with tqdm(total=total_rows, desc="Hashing...") as pbar:
        for batch in inf.iter_batches(batch_size=chunk_size, columns=[text_column]):
            # Convert text_column of batch to list of strings (los)
            texts = batch.column(0).to_pylist()
            process_chunk(texts, chunk_count, out_dir, num_cores)
            chunk_count += 1
            pbar.update(len(texts))
    
    print(f"\nComplete! All .npy files saved to: {out_dir}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", type=str, default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output_dir", type=str, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--text_column", type=str, default=DEFAULT_TEXT_COLUMN)
    parser.add_argument("--num_perm", type=int, default=DEFAULT_NUM_PERM)
    parser.add_argument("--chunk_size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--num_cores", type=int, default=DEFAULT_NUM_CORES)
    args = parser.parse_args()
    
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    else:
        shutil.rmtree(args.output_dir)
        os.makedirs(args.output_dir)
    
    inf = pq.ParquetFile(args.input_file)
    total_rows = inf.metadata.num_rows
    
    hashing(inf, total_rows, args.text_column, args.output_dir, args.num_perm, args.chunk_size, args.num_cores)

if __name__ == "__main__":
    main()
