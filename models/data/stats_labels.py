import polars as pl

def stats_labels(pf: pl.LazyFrame, split="train") -> pl.LazyFrame:
    (
        pf
        .group_by("labels")
        .agg(
            pl.len().alias("count"),
        )
        .collect(engine="streaming")
        .write_csv(f"{split}_labels.csv")
    )

def main():
    for split in ["train", "val", "test"]:
        pf = pl.scan_parquet(f"{split}.parquet")
        stats_labels(pf, split)

if __name__ == "__main__":
    main()
