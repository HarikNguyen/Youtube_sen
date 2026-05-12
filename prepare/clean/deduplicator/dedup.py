import argparse
import polars as pl

DEFAULT_ROOT_FILE = "norm_filtered_comments.parquet"
DEFAULT_OUT_FILE = "deduplicated_comments.parquet"
DEFAULT_BATCH_SIZE = 500_000

def find_all_duplicates(compact_lf):
    all_dup_pairs = []

    for i in range(16):
        print(f"Checking band {i}...")
        band_dups = (
            compact_lf.select(["video_id", "year", "comment_id", f"band_{i}"])
            .group_by(["video_id", "year", f"band_{i}"])
            .agg(pl.col("comment_id").alias("ids"))
            .filter(pl.col("ids").list.len() > 1)
            .select("ids")
            .collect(engine="streaming")
        )
        
        # With each group of duplicates, get the first ID and append the rest
        for row in band_dups.iter_rows():
            ids = row[0]
            canonical = ids[0]
            for duplicate in ids[1:]:
                all_dup_pairs.append(duplicate)

    return set(all_dup_pairs)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--root_file", type=str, default=DEFAULT_ROOT_FILE)
    parser.add_argument("--out_file", type=str, default=DEFAULT_OUT_FILE)
    args = parser.parse_args()

    compact_lf = pl.scan_parquet("lsh_bands.parquet")
    ids_to_remove = find_all_duplicates(compact_lf)

    df_to_remove = pl.DataFrame({"comment_id": list(ids_to_remove)})

    # Filter raw file by removing IDs in df_to_remove
    (
        pl.scan_parquet(args.root_file)
        .join(df_to_remove.lazy(), on="comment_id", how="anti")
        .sink_parquet(args.out_file, engine="streaming", row_group_size=args.batch_size)
    )
    print(f"Saved deduplicated file to {args.out_file}")


if __name__ == "__main__":
    main()
