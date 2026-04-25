import polars as pl
import fasttext

INPUT_CSV = "non_vi_comments.csv"
model = fasttext.load_model("lid.176.bin")
BATCH_SIZE = 500_000


def get_lang_df(texts, k=2):
    print(f"Predicting languages for {len(texts):,} texts...")
    clean_texts = [str(t).replace("\n", " ").replace("\r", " ") for t in texts]
    labels, probs = model.predict(clean_texts, k=k)

    data = {}
    for i in range(k):
        data[f"lang_{i+1}"] = [
            l[i].replace("__label__", "") if len(l) > i else "" for l in labels
        ]
        data[f"prob_{i+1}"] = [p[i] if len(p) > i else 0.0 for p in probs]

    return pl.DataFrame(data)


def get_non_vi_lang(lazy_df, total_rows, batch_size=BATCH_SIZE):
    for offset in range(0, total_rows, batch_size):
        print(f"--- Processing batch: {offset:,} / {total_rows:,} ---")
        chunk = (
            lazy_df.slice(offset, batch_size)
            .select(["comment_id", "comment"])
            .collect()
        )

        if chunk.height == 0:
            continue

        texts = chunk["comment"].to_list()
        lang_df = get_lang_df(texts, k=2)

        result_df = pl.concat([chunk, lang_df], how="horizontal")

        with open("non_vi_lang.csv", mode="ab") as f:
            result_df.select(
                ["comment_id", "comment", pl.col("^lang_.*$"), pl.col("^prob_.*$")]
            ).write_csv(f, include_header=(f.tell() == 0))

        print(f"--- Finished batch: {offset:,} / {total_rows:,} ---")


if __name__ == "__main__":
    lazy_df = pl.scan_csv(INPUT_CSV)

    total_rows = lazy_df.select(pl.len()).collect().item()

    print(f"Total rows: {total_rows:,}")

    get_non_vi_lang(lazy_df, total_rows)

    print("Done!")
