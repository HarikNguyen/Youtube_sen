import os
import argparse
import random
import regex
import polars as pl
import fasttext

DEFAULT_IN_FILE = "latin_comments.csv"
DEFAULT_FINETUNE_FILE = "vi_la_examples.txt"
DEFAULT_PRETRAINED_MODEL = "slang_clr.bin"
DEFAULT_OUT_FILE = "deep_vi_comments.parquet"
DEFAULT_BATCH_SIZE = 500_000

def train_slang_classifier(train_file: str, model_output: str = "slang_clr.bin"):
    """Train a FastText classifier with a given training dataset which iid samples from from vi_only and other_latin_only comments."""
    if not os.path.exists(train_file):
        print(f"File {train_file} does not exist.")
        return None, False

    print(f"Training FastText classifier with data from {train_file}...")
    model = fasttext.train_supervised(
        input=train_file,
        lr=0.5,  # High learning rate for faster convergence on small dataset
        epoch=50,  # Num of training epochs
        wordNgrams=2,  # Capture 2-grams for slang patterns
        minn=2,  # Capture the acronym of slang (ex: "k", "ko", "hk")
        maxn=7,  # Capture the long phrase of slang (vd: "khummm", "chời")
        dim=50,  # Smaller embedding size for faster training and inference even CPU
        loss="hs",  # Hierarchical Softmax for better performance on small datasets
    )
    print(f"Model trained and saved to {model_output}")

    return model, True

def get_slang_classifier(model_output: str = "slang_clr.bin"):
    if not os.path.exists(model_output):
        return None

    print(f"Loading FastText classifier from {model_output}...")
    model = fasttext.load_model(model_output)
    print(f"FastText classifier loaded")

    return model

def detect_slang_vi(s: pl.Series, model) -> pl.Series:
    texts = s.fill_null("").to_list()
    labels, probs = model.predict(texts)
    return pl.Series(
        [
            {"pred_label": lbl[0].replace("__label__", ""), "confidence": float(p[0])}
            for lbl, p in zip(labels, probs)
        ]
    )

def filter_vi_slang(lf, model, outfile, batch_size):
    print("Extracting Vietnamese slang comments...")
    vietnamese_chars = (
        "a-z0-9áàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđ"
    )

    # soft_clean
    lf = lf.with_columns(
        pl.col("comment")
        .str.to_lowercase()
        .str.replace_all(r"[\n\r]", " ")
        .str.replace_all(f"[^{vietnamese_chars}]", " ")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .alias("cleaned_comment")
    )

    # predict
    lf = lf.with_columns(
        pl.col("cleaned_comment")
        .map_batches(
            lambda s: detect_slang_vi(s, model),
            return_dtype=pl.Struct(
                [
                    pl.Field("pred_label", pl.String),
                    pl.Field("confidence", pl.Float64),
                ]
            ),
        )
        .alias("pred")
    ).unnest("pred")

    # filter
    lf = lf.filter(pl.col("pred_label") == "vi").select(["comment_id", "comment"])
    lf.sink_parquet(outfile, row_group_size=batch_size)

def main():
    parser = argparse.ArgumentParser(description="Filter non-Vietnamese comments.")
    parser.add_argument("--input", type=str, default=DEFAULT_IN_FILE, help="Input file")
    parser.add_argument("--train_text", type=str, default=DEFAULT_FINETUNE_FILE, help="Input file to finetuning classifier")
    parser.add_argument("--output_bin", type=str, default=DEFAULT_PRETRAINED_MODEL, help="Output file for pretrained model")
    parser.add_argument("--output", type=str, default=DEFAULT_OUT_FILE, help="Output file")
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size")

    args = parser.parse_args()
    
    # open file
    print(f"Opening file {args.input}...")
    lf = pl.scan_csv(args.input)
    
    # train model
    model, is_trained = train_slang_classifier(args.train_text, args.output_bin)
    
    # filter
    filter_vi_slang(lf, model, args.output, args.batch_size)

    print("Filtering completed successfully.")


if __name__ == "__main__":
    main()
