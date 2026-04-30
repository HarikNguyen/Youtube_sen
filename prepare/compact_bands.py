import polars as pl


def build_bands(lf):
    band_exprs = []
    for i in range(16):
        cols = [pl.col(f"h_{i*8 + j}") for j in range(8)]
        expr = pl.concat_str(cols).hash().alias(f"b_{i}")
        band_exprs.append(expr)
    
    return lf.select([
        "video_id", "year", "comment_id", *band_exprs
    ])

def main():
    lf = pl.scan_parquet("min_hash.parquet")
    build_bands(lf).sink_parquet("lsh_compact.parquet", engine="streaming", row_group_size=500_000)


if __name__ == "__main__":
    main()
