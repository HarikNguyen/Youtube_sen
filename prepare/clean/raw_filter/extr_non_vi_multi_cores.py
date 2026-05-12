import os
import argparse
import concurrent.futures
import multiprocessing
import pyarrow.parquet as pq
import polars as pl
import fasttext

DEFAULT_MODEL_PATH = "glotlid_v1.bin"
DEFAULT_IN_FILE = "raw_comments.parquet"
DEFAULT_OUT_FILE = "non_vi_comments.csv"

# Global variable to hold the model inside worker processes
worker_model = None

def get_total_ram_gb():
    """Attempts to determine system RAM in GB dynamically without external dependencies."""
    try:
        import psutil
        return psutil.virtual_memory().total / (1024**3)
    except ImportError:
        try:
            # Fallback for POSIX systems (Linux/macOS)
            pages = os.sysconf('SC_PHYS_PAGES')
            page_size = os.sysconf('SC_PAGE_SIZE')
            return (pages * page_size) / (1024**3)
        except Exception:
            return 8.0  # Conservative fallback if metrics cannot be accessed

def get_system_strategy():
    """
    Dynamically adjusts batch size and worker count based on available hardware.
    Balances between a high-end server (>30 cores, >250GB RAM) and a low-end machine (4 cores, 8GB RAM).
    """
    ram_gb = get_total_ram_gb()
    cores = os.cpu_count() or 1

    if ram_gb > 64 and cores >= 16:
        # High-end machine profile
        return {
            "batch_size": 250_000,
            "workers": max(1, cores - 2),
            "prefetch": cores * 2  # Keep workers busy without exploding memory
        }
    elif ram_gb <= 8 or cores <= 4:
        # Low-end machine profile
        return {
            "batch_size": 500_000,
            "workers": 1,
            "prefetch": 1
        }
    else:
        # Mid-range machine profile
        return {
            "batch_size": 250_000,
            "workers": max(1, cores // 2),
            "prefetch": max(2, cores)
        }

def init_worker(model_path):
    """
    Initializes the fasttext model in the global scope of each worker process.
    This prevents the need to serialize/deserialize the heavy C++ model object.
    """
    global worker_model
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found: {model_path}")
    worker_model = fasttext.load_model(model_path)

def process_batch(arrow_batch, threshold=0.5):
    """
    Processes a single pyarrow RecordBatch.
    Executes entirely within the worker process to parallelize CPU load.
    """
    df = pl.from_arrow(arrow_batch)

    if df.height == 0:
        return None

    # Eager evaluation of text cleaning
    df = df.filter(
        pl.col("comment").is_not_null() & pl.col("comment").str.contains(r"\p{L}")
    ).with_columns(
        clean_text=pl.col("comment").str.replace_all(r"[\n\r\t]+", " ")
    )

    if df.height == 0:
        return None

    texts = df["clean_text"].to_list()

    # predict and create boolean mask
    labels, probs = worker_model.predict(texts, k=1)
    is_vi = [
        (l[0] == "__label__vie_Latn" and p[0] > threshold)
        for l, p in zip(labels, probs)
    ]

    # Filter out Vietnamese texts
    non_vi_df = df.filter(~pl.Series(is_vi)).select(["comment_id", "comment"])

    if non_vi_df.height == 0:
        return None

    # Return as PyArrow Table for safe and efficient Inter-Process Communication (IPC)
    return non_vi_df.to_arrow()

def run_pipeline(input_path, output_path, model_path):
    """Orchestrates the chunking, parallel processing, and sequential writing."""
    strategy = get_system_strategy()
    print(f"Hardware Strategy Applied: {strategy['workers']} workers, {strategy['batch_size']} batch size.")

    if os.path.exists(output_path):
        os.remove(output_path)

    parquet_file = pq.ParquetFile(input_path)
    batch_iter = parquet_file.iter_batches(batch_size=strategy["batch_size"])
    
    first_write = True
    total_processed_rows = 0

    if strategy["workers"] <= 1:
        # Sequential Execution (Strict memory bounds for 8GB RAM weak machines)
        init_worker(model_path)
        for batch in batch_iter:
            result_arrow = process_batch(batch)
            if result_arrow is not None:
                result_df = pl.from_arrow(result_arrow)
                with open(output_path, mode="ab") as f:
                    result_df.write_csv(f, include_header=first_write)
                first_write = False
            
            total_processed_rows += batch.num_rows
            print(f"Processed chunk... Total rows read: {total_processed_rows:,}")
            
    else:
        # Parallel Execution (High CPU utilization for 30+ core strong machines)
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=strategy["workers"],
            initializer=init_worker,
            initargs=(model_path,)
        ) as executor:
            
            futures = set()

            def submit_next_batch():
                try:
                    batch = next(batch_iter)
                    futures.add(executor.submit(process_batch, batch))
                    return batch.num_rows
                except StopIteration:
                    return 0

            # Fill the initial prefetch queue
            for _ in range(strategy["prefetch"]):
                submit_next_batch()

            # Process futures as they complete to keep memory stable
            while futures:
                done, futures = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )
                
                for future in done:
                    try:
                        result_arrow = future.result()
                        if result_arrow is not None:
                            # Safely write to CSV sequentially in the main process
                            result_df = pl.from_arrow(result_arrow)
                            with open(output_path, mode="ab") as f:
                                result_df.write_csv(f, include_header=first_write)
                            first_write = False
                    except Exception as e:
                        print(f"Worker exception: {e}")

                    # Submit exactly one new batch for every completed batch
                    rows_added = submit_next_batch()
                    if rows_added > 0:
                        total_processed_rows += rows_added
                        print(f"Streaming data... Total rows read into pool: {total_processed_rows:,}")

def main():
    parser = argparse.ArgumentParser(description="Filter non-Vietnamese comments dynamically.")
    parser.add_argument("--input", type=str, default=DEFAULT_IN_FILE)
    parser.add_argument("--output", type=str, default=DEFAULT_OUT_FILE)
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL_PATH)

    args = parser.parse_args()

    try:
        run_pipeline(args.input, args.output, args.model)
        print("Filtering completed successfully.")
    except Exception as e:
        print(f"Error during execution: {e}")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
