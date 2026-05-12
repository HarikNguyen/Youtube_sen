import os
import argparse
import polars as pl
import fasttext

DEFAULT_MODEL_PATH = "glotlid_v1.bin"
DEFAULT_IN_FILE = "raw_comments.parquet"
DEFAULT_OUT_FILE = "non_vi_comments.csv"
DEFAULT_BATCH_SIZE = 500_000


def load_extractor(model_path):
    """Loads the FastText model with error handling."""
    if not os.path.exists(model_path):
        raise FileNotFoundError(
            f"Model file not found: {model_path}\nYou can download it via `wget https://huggingface.co/cis-lmu/glotlid/resolve/main/model.bin -O g.bin`"
        )
    return fasttext.load_model(model_path)


def get_vietnamese_mask(texts, model, threshold=0.5):
    """
    Predicts language for a list of strings.
    Returns a boolean list: True if Vietnamese (vie_Latn).
    """
    if not texts:
        return []

    labels, probs = model.predict(texts, k=1)
    # GlotLID label for Vietnamese is '__label__vie_Latn'
    return [
        (l[0] == "__label__vie_Latn" and p[0] > threshold)
        for l, p in zip(labels, probs)
    ]


def process_and_save_chunks(lazy_df, model, output_path, batch_size):
    """
    Processes the LazyFrame in chunks to keep memory usage low.
    Filters out Vietnamese comments and saves the rest to CSV.
    """
    if os.path.exists(output_path):
        os.remove(output_path)

    total_rows = lazy_df.select(pl.len()).collect().item()

    # Pre-optimize the LazyFrame: filter nulls and clean whitespace
    # This runs in Rust/Polars before hitting Python
    optimized_lf = lazy_df.filter(
        pl.col("comment").is_not_null() & pl.col("comment").str.contains(r"\p{L}")
    ).with_columns(clean_text=pl.col("comment").str.replace_all(r"[\n\r\t]+", " "))

    for offset in range(0, total_rows, batch_size):
        # Fetch chunk
        chunk = optimized_lf.slice(offset, batch_size).collect()
        if chunk.height == 0:
            continue

        # Detection logic
        is_vi = get_vietnamese_mask(chunk["clean_text"].to_list(), model)

        # Filter: Keep only non-Vietnamese
        non_vi_df = chunk.filter(~pl.Series(is_vi)).select(["comment_id", "comment"])

        if non_vi_df.height > 0:
            with open(output_path, mode="ab") as f:
                non_vi_df.write_csv(f, include_header=(offset == 0))

        print(f"Processed {offset + chunk.height:,} / {total_rows:,} rows.")


def run_pipeline(input_path, output_path, model_path, batch_size):
    """Orchestrates the filtering process."""
    model = load_extractor(model_path)
    lazy_df = pl.scan_parquet(input_path)

    process_and_save_chunks(
        lazy_df=lazy_df, model=model, output_path=output_path, batch_size=batch_size
    )


def main():
    parser = argparse.ArgumentParser(description="Filter non-Vietnamese comments.")
    parser.add_argument("--input", type=str, default=DEFAULT_IN_FILE)
    parser.add_argument("--output", type=str, default=DEFAULT_OUT_FILE)
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL_PATH)
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE)

    args = parser.parse_args()

    try:
        run_pipeline(args.input, args.output, args.model, args.batch_size)
        print("Filtering completed successfully.")
    except Exception as e:
        print(f"Error during execution: {e}")


if __name__ == "__main__":
    main()
