import regex
import polars as pl


def script_filter(text):
    if not text or not text.strip():
        return "empty", 0

    # Loại bỏ các ký tự không phải chữ cái/số (khoảng trắng, icon, dấu câu)
    clean_text = "".join(regex.findall(r"\p{L}|\p{N}", text))
    if not clean_text:
        return "non_language", 0

    total_len = len(clean_text)

    latin_chars = regex.findall(r"\p{Script=Latin}", clean_text)

    latin_ratio = (len(latin_chars) / total_len) * 100

    if latin_ratio > 70:
        return "LATIN_GROUP", latin_ratio
    else:
        return "NON_LATIN_GROUP", latin_ratio


if __name__ == "__main__":

    lazy_df = pl.scan_csv("non_vi_comments_glotlid.csv")

    total_rows = lazy_df.select(pl.len()).collect().item()

    for offset in range(0, total_rows, 500_000):
        chunk = (
            lazy_df.slice(offset, 500_000)
            .select(["comment_id", "comment"])
            .filter(pl.col("comment").is_not_null())
            .collect()
        )

        if chunk.height == 0:
            continue

        texts = chunk["comment"].to_list()
        results = [script_filter(t) for t in texts]

        latin_chunk = chunk.filter(pl.Series([r[0] == "LATIN_GROUP" for r in results]))

        with open("latin_comments_glotlid.csv", mode="ab") as f:
            latin_chunk.select(["comment_id", "comment"]).write_csv(
                f, include_header=(f.tell() == 0)
            )

        print(f"Processed batch: {offset:,} / {total_rows:,}")

    print("Done!")
