import os
import polars as pl
import fasttext

INPUT_FILE = "raw_comments.parquet"
OUTPUT_FILE = "non_vi_comments.csv"
MODEL_PATH = "lid.176.bin"
BATCH_SIZE = 500_000

model = fasttext.load_model(MODEL_PATH)


def detect_vi_batch(texts):
    if not texts:
        return []
    clean_texts = [str(t).replace("\n", " ").replace("\r", " ") for t in texts]
    labels, probs = model.predict(clean_texts, k=1)
    return [
        l[0] == "__label__vi" if p[0] > 0.5 else False for l, p in zip(labels, probs)
    ]


def pre_filter(lazy_df, total_rows, batch_size=BATCH_SIZE):
    for offset in range(0, total_rows, batch_size):
        print(f"--- Processing batch: {offset:,} / {total_rows:,} ---")
        chunk = (
            lazy_df.slice(offset, batch_size)
            .select(["comment_id", "comment"])
            .filter(
                pl.col("comment").is_not_null()
                & pl.col("comment").str.contains(r"\p{L}")
            )
            .collect()
        )

        if chunk.height == 0:
            continue

        texts = chunk["comment"].to_list()
        is_vi = detect_vi_batch(texts)

        non_vi_chunk = chunk.filter(~pl.Series(is_vi))

        with open(OUTPUT_FILE, mode="ab") as f:

            non_vi_chunk.select(["comment_id", "comment"]).write_csv(
                f, include_header=(f.tell() == 0)
            )


if __name__ == "__main__":
    lazy_df = pl.scan_parquet(INPUT_FILE)
    total_rows = lazy_df.select(pl.len()).collect().item()
    print(f"Total rows: {total_rows:,}")

    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    pre_filter(lazy_df, total_rows)

    print("Done!")
