import polars as pl

NUM = 10000
lazy_df = pl.scan_parquet(f"sampled_{NUM}.parquet")
lazy_df.sink_csv(
    f"sampled_{NUM}.csv",
    batch_size=500_000,
    include_header=True,
)
