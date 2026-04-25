import os
import polars as pl
import fasttext

INPUT_FILE = "raw_comments.parquet"
OUTPUT_FILE = "non_vi_comments_glotlid.csv"
MODEL_PATH = "glotlid_v1.bin"
BATCH_SIZE = 500_000

# Load model GlotLID (FastText backend)
model = fasttext.load_model(MODEL_PATH)


def detect_vi_batch(texts):
    if not texts:
        return []

    # Tiền xử lý nhẹ để model không bị nhiễu bởi xuống dòng
    clean_texts = [str(t).replace("\n", " ").replace("\r", " ") for t in texts]

    # GlotLID dự đoán
    labels, probs = model.predict(clean_texts, k=1)

    is_vi_list = []
    for l, p in zip(labels, probs):
        label = l[0]
        score = p[0]

        # Nếu là tiếng Việt chuẩn với xác suất > 0.5
        if label == "__label__vie_Latn" and score > 0.5:
            is_vi_list.append(True)
        else:
            is_vi_list.append(False)

    return is_vi_list


def pre_filter(lazy_df, total_rows, batch_size=BATCH_SIZE):
    for offset in range(0, total_rows, batch_size):
        print(f"--- Processing batch: {offset:,} / {total_rows:,} ---")

        chunk = (
            lazy_df.slice(offset, batch_size)
            .select(["comment_id", "comment"])
            .filter(
                pl.col("comment").is_not_null()
                & pl.col("comment").str.contains(
                    r"\p{L}"
                )  # Phải chứa ít nhất 1 chữ cái
            )
            .collect()
        )

        if chunk.height == 0:
            continue

        texts = chunk["comment"].to_list()
        is_vi = detect_vi_batch(texts)

        # Lọc ra những dòng KHÔNG PHẢI tiếng Việt (bao gồm Bengali, Anh, Pháp, rác...)
        non_vi_chunk = chunk.filter(~pl.Series(is_vi))

        if non_vi_chunk.height > 0:
            with open(OUTPUT_FILE, mode="ab") as f:
                non_vi_chunk.select(["comment_id", "comment"]).write_csv(
                    f, include_header=(f.tell() == 0)
                )

        print(f"-> Batch done. Kept {non_vi_chunk.height:,} non-vi rows.")


if __name__ == "__main__":
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"Thiếu file model GlotLID tại: {MODEL_PATH}")

    lazy_df = pl.scan_parquet(INPUT_FILE)
    total_rows = lazy_df.select(pl.len()).collect().item()
    print(f"Total rows in parquet: {total_rows:,}")

    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    pre_filter(lazy_df, total_rows)

    print("\n--- Done! Step 1 with GlotLID finished. ---")
