import os
import polars as pl
from datasketch import MinHash

# import polars_hash as plh

INPUT_FILE = "norm_filtered_comments.parquet"
OUTPUT_FILE = "deduplicated_comments.parquet"
BATCH_SIZE = 500_000
NUM_HASHES = 128

############################################################################################
##                                         UDFs                                           ##
############################################################################################


def get_minhash_signature(text: str) -> list[int] | None:
    if not text or not isinstance(text, str):
        return None

    m = MinHash(num_perm=NUM_HASHES)

    for word in text.split(" "):
        if word:
            m.update(word.encode("utf8"))

    return [int(x) for x in m.hashvalues]


def deduplicate_by_video_with_minhash(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Deduplicate comments per video natively via Rust engine using MinHash.
    (Optimized: No explode, uses List expressions)
    """

    lf_signature = (
        lf.select(["comment_id", "video_id", "comment"])
        .with_columns(
            pl.col("comment")
            .map_elements(get_minhash_signature, return_dtype=pl.List(pl.Int64))
            .alias("signature")
        )
        .drop("comment")
    )
    lf_signature = lf_signature.drop_nulls(subset=["signature"])
    lf_deduped = (
        lf_signature.group_by(["video_id", "signature"])
        .agg(pl.col("comment_id").first().alias("choosen_comment_id"))
        .drop("signature")
    )

    return lf_deduped
    # return (
    # lf
    # .select(["comment_id", "video_id", "comment"])
    # .with_columns(
    # _sha256=plh.col("comment").chash.sha2_256()
    # )
    # .drop("comment")
    # .group_by(["video_id", "_sha256"])
    # .agg(pl.col("comment_id").first().alias("choosen_comment_id"))
    # .drop("_sha256") # Drops the hash, leaves video_id and choosen_comment_id
    # )


############################################################################################
##                                     Pipelines                                          ##
############################################################################################


def extract_unique_ids(input_path: str, temp_path: str, batch_size: int) -> None:
    """Phase 1: Extract unique comment IDs per video."""
    lf = pl.scan_parquet(input_path).filter(pl.col("comment").is_not_null())

    processed_lf = lf.pipe(deduplicate_by_video_with_minhash)

    print("Phase 1: Executing processing and writing temp file...")
    processed_lf.sink_parquet(temp_path, row_group_size=batch_size)
    print(f"Phase 1: Complete...")


def filter_by_comment_ids(
    input_path: str, choosen_path: str, output_path: str, batch_size: int
) -> None:
    """Phase 2: Filter comments by unique comment IDs."""
    lf_orig = pl.scan_parquet(input_path).filter(pl.col("comment").is_not_null())
    lf_choosen = pl.scan_parquet(choosen_path)

    lf_final = lf_orig.join(
        lf_choosen, left_on="comment_id", right_on="choosen_comment_id", how="semi"
    )

    print("Phase 2: Executing processing and writing file...")
    lf_final.sink_parquet(output_path, row_group_size=batch_size)
    print(f"Phase 2: Complete!!!\nResults saved to {OUTPUT_FILE}")


############################################################################################
##                                       Main Flow                                        ##
############################################################################################


def main():
    extract_unique_ids(INPUT_FILE, "dedup_temp.parquet", BATCH_SIZE)
    filter_by_comment_ids(INPUT_FILE, "dedup_temp.parquet", OUTPUT_FILE, BATCH_SIZE)
    if os.path.exists("dedup_temp.parquet"):
        os.remove("dedup_temp.parquet")


if __name__ == "__main__":
    main()
