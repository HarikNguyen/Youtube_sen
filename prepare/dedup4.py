import os
import re
import regex
import polars as pl
from tqdm import tqdm

INPUT_FILE = "raw_filtered_comments.parquet"
OUTPUT_CLEAN = "cleaned_deduplicated_comments.parquet"
OUTPUT_DEDUP_LOG = "dedup.csv"
BATCH_SIZE = 1_000_000

###########################################################################################
#######################################  UDFs #############################################
###########################################################################################


def detect_junk_udf(s: pl.Series) -> pl.Series:
    """User Defined Function (UDF) to detect if a given text is junk (rác nghệ thuật)."""
    results = []
    for text in s.to_list():
        t = str(text)
        if not t or len(t) < 2:
            results.append(True)
            continue

        # Char/Number ratio
        alpha_chars = regex.findall(r"\p{L}|\p{N}", t)
        alpha_ratio = len(alpha_chars) / len(t) if len(t) > 0 else 0

        # Check art blocks
        has_blocks = bool(regex.search(r"[\u2500-\u25FF]", t))
        line_count = t.count("\n") + 1

        is_junk = (
            has_blocks
            or (line_count > 4 and alpha_ratio < 0.4)
            or (len(t) > 50 and alpha_ratio < 0.2)
            or any(len(word) > 40 for word in t.split())
        )
        results.append(is_junk)
    return pl.Series(results, dtype=pl.Boolean)


def collapse_characters(s: pl.Series) -> pl.Series:
    """
    Collapses repeated characters.
    Solve the 'backreferences' of Rust Regex in Polars.

    """
    pattern = re.compile(r"(.)\1+")
    return pl.Series(
        [pattern.sub(r"\1", str(val)) if val else val for val in s.to_list()]
    )


###########################################################################################
################################### Deduplication #########################################
###########################################################################################


def run_cleanup_step_2():
    if not os.path.exists(INPUT_FILE):
        print(f"Not found input file: {INPUT_FILE}")
        return

    # Scan file
    lazy_df = pl.scan_parquet(INPUT_FILE)
    total_rows = lazy_df.select(pl.len()).collect().item()

    print("Processing {} comments...".format(total_rows))
    collected_chunks = []

    # A: FILTER JUNK
    with tqdm(
        total=total_rows, desc="Cleaning Artist Junk", unit="row", colour="green"
    ) as pbar:
        for offset in range(0, total_rows, BATCH_SIZE):
            chunk = lazy_df.slice(offset, BATCH_SIZE).collect()

            clean_chunk = (
                chunk.with_columns(
                    is_junk=pl.col("comment").map_batches(detect_junk_udf)
                )
                .filter(pl.col("is_junk") == False)
                .drop("is_junk")
            )
            collected_chunks.append(clean_chunk)
            pbar.update(len(chunk))

    # Union all
    df_all = pl.concat(collected_chunks)
    del collected_chunks

    print("\nProcessing and Analyzing...")

    # --- B: CREATE FINGERPRINTING ---
    df_with_fp = df_all.with_columns(
        fingerprint=pl.col("comment")
        .str.to_lowercase()
        .str.replace_all(r"[^\p{L}\p{N}]", "")  # Xóa ký tự đặc biệt (Rust)
        .map_batches(collapse_characters)  # Thu gọn ký tự lặp (Python Re)
    )

    # --- C: EXTRACT DUPLICATED ---
    print("Extracting duplicated...")

    # Một comment bị coi là trùng nếu:
    # 1. Comment đó giống hệt một comment khác.
    # 2. Hoặc fingerprint của nó giống một comment khác (biến thể lặp chữ).

    mask_duplicated = df_with_fp.filter(
        pl.col("comment").is_duplicated().over("video_id")
        | pl.col("fingerprint").is_duplicated().over("video_id")
    )

    dedup_log = mask_duplicated.select(["comment_id", "video_id", "comment"]).unique(
        subset=["comment_id"]
    )  # Đảm bảo không trùng ID trong file log

    dedup_log.write_csv(OUTPUT_DEDUP_LOG)
    print(f"Export {dedup_log.height:,} duplicated comments to {OUTPUT_DEDUP_LOG}")


if __name__ == "__main__":
    run_cleanup_step_2()
