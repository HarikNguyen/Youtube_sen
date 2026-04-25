import os
import random
import regex
import polars as pl
import fasttext

INPUT_FILE = "raw_comments.parquet"
OUTPUT_FILE = "comments_no_vi.parquet"
MODEL_PATH = "glotlid_v1.bin"
BATCH_SIZE = 500_000

###########################################################################################
####################################### UDFs ##############################################
###########################################################################################


def detect_vi_udf(s: pl.Series, model) -> pl.Series:
    """User Defined Function (UDF) to detect if a given text is in Vietnamese using a pre-trained FastText language identification model."""
    texts = [str(t).replace("\n", " ").replace("\r", " ") for t in s.to_list()]

    if not texts:
        return pl.Series([], dtype=pl.Boolean)

    labels, probs = model.predict(texts, k=1)

    is_vi = [
        (l[0] == "__label__vie_Latn" and p[0] > 0.5) for l, p in zip(labels, probs)
    ]

    return pl.Series(is_vi, dtype=pl.Boolean)


def detect_latin_script_udf(s: pl.Series) -> pl.Series:
    """User Defined Function (UDF) to detect if a given text contains Latin script characters."""
    results = []
    for text in s.to_list():
        t = str(text)
        if not t or not t.strip():
            results.append(False)
            continue

        clean_text = "".join(regex.findall(r"\p{L}|\p{N}", t))
        if not clean_text:
            results.append(False)
            continue

        latin_chars = regex.findall(r"\p{Script=Latin}", clean_text)
        latin_ratio = (len(latin_chars) / len(clean_text)) * 100

        results.append(latin_ratio > 70)

    return pl.Series(results, dtype=pl.Boolean)


def clean_text_udf(s: pl.Series) -> pl.Series:
    """User Defined Function (UDF) to clean text by:
    - Removing icons/emojis
    - Removing repeated words (e.g., "vui vui vui" -> "vui")
    - Handling extra whitespace
    - Lowercasing (already done in Polars for efficiency)
    """
    cleaned = []
    for text in s.to_list():
        t = str(text)
        t = regex.sub(r"[^\p{L}\p{N}\p{P}\s]", "", t)
        t = regex.sub(r"\b(\w+)(?:\s+\1\b)+", r"\1", t, flags=regex.IGNORECASE)
        t = regex.sub(r"\s+", " ", t).strip()
        cleaned.append(t)

    return pl.Series(cleaned, dtype=pl.String)


def clean_vietnamese_expr(col_name: str):
    """User Defined Function (UDF) to deep clean"""
    vietnamese_chars = (
        "a-z0-9áàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđ"
    )
    return (
        pl.col(col_name)
        .str.to_lowercase()
        .str.replace_all(r"[\n\r]", " ")
        # Giữ lại các ký tự tiếng Việt, số và khoảng trắng
        .str.replace_all(f"[^{vietnamese_chars} ]", " ")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
    )


def detect_slang_vi(s: pl.Series, model) -> pl.Series:
    texts = s.to_list()
    labels, probs = model.predict(texts)
    return pl.Series(
        [
            {"pred_label": lbl[0].replace("__label__", ""), "confidence": float(p[0])}
            for lbl, p in zip(labels, probs)
        ]
    )


###########################################################################################
############################# CUSTOM FASTTEXT SLANG CLASSIFIER ############################
###########################################################################################


def train_slang_classifier(train_file: str, model_output: str = "slang_clr.bin"):
    """Train a FastText classifier with a given training dataset which iid samples from from vi_only and other_latin_only comments."""
    if not os.path.exists(train_file):
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


def manual_select_slang_comments(potential_list: list) -> list:
    """Manually select comments.
    This function have 4 mode:
    1. all (ra/sa): Remove all or Select All
    2. index (ri <index>/si <index>): Remove or Select comment by index in the potential list.
    3. range (rr <start> <end>/sr <start> <end>): Remove or Select a range of comments by index in the potential list.
    4. list (rl <index1> <index2> ... <indexN>/sl <index1> <index2> ... <indexN>): Remove or Select multiple comments by index in the potential list.
    """
    try:
        val = input("Enter the number of comments to review (e.g., 20): ").strip()
        split_num = int(val) if val else 20
    except ValueError:
        split_num = 20

    final_selected = []
    total = len(potential_list)

    for i in range(0, total, split_num):
        batch = potential_list[i : i + split_num]
        current_len = len(batch)

        os.system("cls" if os.name == "nt" else "clear")
        print("\n" + "═" * 60)
        for idx, item in enumerate(batch):
            print(f"[{idx}] {item}")

        print("═" * 60)
        print(
            f"That shows comments from {i*split_num} to {min(total, (i+1)*split_num)} out of {total}."
        )
        print(
            "Commands <<< [sa/ra] [si/ri <idx>] [sr/rr <start> <end>] [sl/rl <i1> <i2>...] | [q] >>>"
        )
        cmd_raw = input("Enter command: ").strip().lower()
        if not cmd_raw:
            continue
        if cmd_raw == "q":
            break

        parts = cmd_raw.split()
        mode = parts[0]
        args = parts[1:]
        indices_to_keep = set()
        all_indices = set(range(current_len))

        try:
            # ALL mode
            if mode == "sa":
                indices_to_keep = all_indices
            elif mode == "ra":
                indices_to_keep = set()

            # Invidual mode
            elif mode == "si":
                indices_to_keep = {int(args[0])}
            elif mode == "ri":
                indices_to_keep = all_indices - {int(args[0])}

            # 3. Range mode
            elif mode == "sr":
                start, end = int(args[0]), int(args[1])
                indices_to_keep = set(range(start, end + 1))
            elif mode == "rr":
                start, end = int(args[0]), int(args[1])
                indices_to_keep = all_indices - set(range(start, end + 1))

            # 4. List mode
            elif mode == "sl":
                indices_to_keep = {int(x) for x in args}
            elif mode == "rl":
                indices_to_keep = all_indices - {int(x) for x in args}

            else:
                print("Command invalid, skip this batch.")
                continue

        except Exception as e:
            print(f"Error while processing command: {e}")
            continue

        for local_idx in sorted(list(indices_to_keep)):
            if 0 <= local_idx < current_len:
                final_selected.append(batch[local_idx])

    return final_selected


###########################################################################################
##################################### MAIN EXECUTION ######################################
###########################################################################################


def run_pipeline():
    # --------------------------- Sampling & Traing -----------------------------
    # Init scan pipeline.
    orig_lp = pl.scan_parquet(INPUT_FILE)
    base_lp = (
        pl.scan_parquet(INPUT_FILE)
        .select([pl.col("comment_id", "comment")])
        .filter(pl.col("comment").is_not_null())
        .with_columns(pl.col("comment").str.to_lowercase())
    )

    # Raw filter
    glotlid_v1_model = fasttext.load_model(MODEL_PATH)
    classified_lp = base_lp.with_columns(
        [
            pl.col("comment")
            .map_batches(lambda s: detect_vi_udf(s, glotlid_v1_model))
            .alias("is_vi"),
            pl.col("comment").map_batches(detect_latin_script_udf).alias("is_latin"),
        ]
    )

    # --------------------------- Prepare and Manual Review ---------------------
    # Get 4000 completely certain vietnamese comments
    vi_list = (
        classified_lp.filter(pl.col("is_vi") == True)
        .head(4000)
        .select("comment")
        .collect()
        .with_columns(
            [
                pl.col("comment").map_batches(clean_text_udf),
                pl.lit("__label__vi").alias("label"),
            ]
        )
        .select((pl.col("label") + " " + pl.col("comment")).alias("final"))
        .get_column("final")
        .to_list()
    )

    # Manually review and filter
    confirmed_vi_list = manual_select_slang_comments(vi_list)

    # Get 6000 uncertain other latin comments_no_vi
    other_latin_list = (
        classified_lp.filter((pl.col("is_vi") == False) & (pl.col("is_latin") == True))
        .head(6000)
        .select("comment")
        .collect()
        .with_columns(
            [
                pl.col("comment").map_batches(clean_text_udf),
                pl.lit("__label__latin").alias("label"),
            ]
        )
        .select((pl.col("label") + " " + pl.col("comment")).alias("final"))
        .get_column("final")
        .to_list()
    )

    # Manually review and filter
    confirmed_other_latin_list = manual_select_slang_comments(other_latin_list)

    # Concatenate, shuffle and save the training data for slang CLASSIFIER
    training_data = confirmed_vi_list + confirmed_other_latin_list
    random.shuffle(training_data)
    with open("slang_clf_train.txt", "w", encoding="utf-8") as f:
        for line in training_data:
            f.write(line + "\n")

    # ---------------------------------- Training -------------------------------
    # Train the slang CLASSIFIER
    slang_clr, is_success = train_slang_classifier(
        "slang_clf_train.txt", "slang_clr.bin"
    )
    if not is_success:
        print("Failed to train slang classifier. Exiting.")
        return

    # ----------------------------- Filter slang vi -----------------------------
    slang_vi_lp = (
        classified_lp.filter((pl.col("is_vi") == False) & (pl.col("is_latin") == True))
        .with_columns(clean_vietnamese_expr("comment").alias("cleaned_comment"))
        .with_columns(
            prediction=pl.col("cleaned_comment").map_batches(
                lambda s: detect_slang_vi(s, slang_clr),
                return_dtype=pl.Struct(
                    [
                        pl.Field("pred_label", pl.String),
                        pl.Field("confidence", pl.Float64),
                    ]
                ),
            )
        )
        .unnest("prediction")
        .filter((pl.col("pred_label") == "vi") & (pl.col("confidence") > 0.6))
        .select(["comment_id", "comment"])
    )

    # --------------------------- Final pipe ------------------------------------
    orig_lp = pl.scan_parquet(INPUT_FILE)
    final_dataset_lp = pl.concat(
        [
            classified_lp.filter(pl.col("is_vi") == True).select(
                ["comment_id", "comment"]
            ),
            slang_vi_lp,
        ]
    ).unique(subset=["comment_id"])

    final_full_fields_lp = orig_lp.join(
        final_dataset_lp.select("comment_id"), on="comment_id", how="semi"
    )

    # ------------------------------ Saving --------------------------------------
    final_full_fields_lp.sink_parquet(OUTPUT_FILE, row_group_size=BATCH_SIZE)
    print(f"Saved final dataset to {OUTPUT_FILE}")


if __name__ == "__main__":
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"Model không tồn tại: {MODEL_PATH}")

    run_pipeline()
