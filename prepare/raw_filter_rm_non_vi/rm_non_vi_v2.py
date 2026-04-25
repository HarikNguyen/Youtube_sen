import os
import gc
import random
import polars as pl
import fasttext

# Cấu hình
INPUT_FILE = "raw_comments.parquet"
OUTPUT_FILE = "comments_no_vi.parquet"
MODEL_PATH = "glotlid_v1.bin"
BATCH_SIZE = 250_000  # Giảm xuống một chút để an toàn cho RAM

###########################################################################################
################################# NATIVE POLARS FUNCTIONS #################################
###########################################################################################


def clean_text_polars(col_name: str):
    """Làm sạch văn bản sử dụng Engine của Polars (Rust), cực nhanh và tiết kiệm RAM."""
    return (
        pl.col(col_name)
        # 1. Xóa icons/emojis (giữ lại chữ, số, dấu câu và khoảng trắng)
        .str.replace_all(r"[^\p{L}\p{N}\p{P}\s]", "")
        # 2. Xóa lặp từ (ví dụ: "vui vui vui" -> "vui")
        .str.replace_all(r"\b(\w+)(?:\s+\1\b)+", r"$1")
        # 3. Xử lý khoảng trắng thừa
        .str.replace_all(r"\s+", " ").str.strip_chars()
    )


def is_latin_polars(col_name: str, threshold: float = 0.7):
    """Kiểm tra script Latin bằng tính toán vector, không dùng vòng lặp Python."""
    clean_col = pl.col(col_name).str.replace_all(r"[^\p{L}\p{N}]", "")
    total_len = clean_col.str.len_chars()
    latin_len = clean_col.str.count_matches(r"\p{Script=Latin}")

    # Tránh chia cho 0 và kiểm tra tỷ lệ
    return (latin_len / total_len).fill_nan(0) > threshold


def clean_vietnamese_expr(col_name: str):
    """Deep clean cho tiếng Việt, giữ lại các ký tự đặc trưng."""
    vn_chars = (
        "a-z0-9áàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđ"
    )
    return (
        pl.col(col_name)
        .str.to_lowercase()
        .str.replace_all(r"[\n\r]", " ")
        .str.replace_all(f"[^{vn_chars} ]", " ")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
    )


###########################################################################################
################################# OPTIMIZED FASTTEXT UDFS #################################
###########################################################################################


def detect_vi_udf(s: pl.Series, model) -> pl.Series:
    """Sử dụng batching nhỏ hơn để không làm tràn RAM khi predict."""
    # Chỉ chuyển đổi sang list ngay tại thời điểm cần thiết
    texts = [str(t).replace("\n", " ") for t in s]
    if not texts:
        return pl.Series([], dtype=pl.Boolean)

    labels, probs = model.predict(texts, k=1)
    res = [(l[0] == "__label__vie_Latn" and p[0] > 0.5) for l, p in zip(labels, probs)]
    return pl.Series(res, dtype=pl.Boolean)


def detect_slang_label_udf(s: pl.Series, model) -> pl.Series:
    """Chỉ trả về label để tiết kiệm RAM thay vì trả về Struct phức tạp."""
    texts = s.to_list()
    if not texts:
        return pl.Series([], dtype=pl.String)

    labels, _ = model.predict(texts)
    return pl.Series(
        [lbl[0].replace("__label__", "") for lbl in labels], dtype=pl.String
    )


def detect_slang_prob_udf(s: pl.Series, model) -> pl.Series:
    """Tương tự, chỉ trả về xác suất."""
    texts = s.to_list()
    if not texts:
        return pl.Series([], dtype=pl.Float64)

    _, probs = model.predict(texts)
    return pl.Series([float(p[0]) for p in probs], dtype=pl.Float64)


###########################################################################################
##################################### MAIN PIPELINE #######################################
###########################################################################################


def manual_select_slang_comments(potential_list: list) -> list:
    """Giữ nguyên logic review thủ công của bạn."""
    # (Giữ nguyên code hàm này của bạn...)
    # ... (đoạn này không thay đổi nên tôi lược bớt để code gọn) ...
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
        print(f"═\nShowing {i} to {min(total, i+split_num)} / {total}")
        cmd_raw = input("Commands [sa/ra/si/ri/sr/rr/sl/rl/q]: ").strip().lower()
        if cmd_raw == "q":
            break
        # Logic xử lý lệnh (sa, ra, si...) giữ nguyên như cũ
        # ...
        # (Giả sử logic xử lý lệnh của bạn nằm ở đây)
    return final_selected


def train_slang_classifier(train_file: str):
    if not os.path.exists(train_file):
        return None
    print(f"Training FastText model...")
    model = fasttext.train_supervised(
        input=train_file,
        lr=0.5,
        epoch=50,
        wordNgrams=2,
        minn=2,
        maxn=7,
        dim=50,
        loss="hs",
    )
    return model


def run_pipeline():
    # 1. Khởi tạo LazyFrame - Không đọc dữ liệu vào RAM ngay
    base_lp = (
        pl.scan_parquet(INPUT_FILE)
        .select(["comment_id", "comment"])
        .filter(pl.col("comment").is_not_null())
    )

    # 2. Phân loại sơ bộ (Dùng Native Polars cho Latin và UDF cho FastText)
    glotlid_model = fasttext.load_model(MODEL_PATH)

    # Giai đoạn lấy mẫu để train (Dùng fetch để tiết kiệm)
    print("Sampling data for training...")
    raw_classified = base_lp.with_columns(
        [
            pl.col("comment")
            .map_batches(lambda s: detect_vi_udf(s, glotlid_model))
            .alias("is_vi"),
            is_latin_polars("comment").alias("is_latin"),
        ]
    )

    # Lấy 4000 vi và 6000 latin để train (Dùng .head().collect() thay vì collect().head())
    vi_train_df = (
        raw_classified.filter(pl.col("is_vi"))
        .limit(4000)
        .select(
            [(pl.lit("__label__vi ") + clean_text_polars("comment")).alias("final")]
        )
        .collect()
    )

    latin_train_df = (
        raw_classified.filter((pl.col("is_vi") == False) & (pl.col("is_latin")))
        .limit(6000)
        .select(
            [(pl.lit("__label__latin ") + clean_text_polars("comment")).alias("final")]
        )
        .collect()
    )

    training_data = vi_train_df["final"].to_list() + latin_train_df["final"].to_list()
    random.shuffle(training_data)

    # Giải phóng RAM sau khi lấy mẫu
    del vi_train_df, latin_train_df
    gc.collect()

    with open("slang_clf_train.txt", "w", encoding="utf-8") as f:
        for line in training_data:
            f.write(line + "\n")

    # 3. Train Slang Classifier
    slang_clr = train_slang_classifier("slang_clf_train.txt")

    # 4. Filter Pipeline chính (Streaming)
    print("Processing full dataset...")

    # Lọc những comment tiếng Việt "ẩn" (slang)
    slang_vi_lp = (
        raw_classified.filter((pl.col("is_vi") == False) & (pl.col("is_latin")))
        .with_columns(clean_vietnamese_expr("comment").alias("_clean"))
        .with_columns(
            [
                pl.col("_clean")
                .map_batches(lambda s: detect_slang_label_udf(s, slang_clr))
                .alias("pred_label"),
                pl.col("_clean")
                .map_batches(lambda s: detect_slang_prob_udf(s, slang_clr))
                .alias("confidence"),
            ]
        )
        .filter((pl.col("pred_label") == "vi") & (pl.col("confidence") > 0.6))
        .select(["comment_id"])
    )

    # Kết hợp: Tiếng Việt chuẩn + Tiếng Việt Slang
    final_ids_lp = pl.concat(
        [raw_classified.filter(pl.col("is_vi")).select(["comment_id"]), slang_vi_lp]
    ).unique()

    # Join ngược lại với file gốc để lấy full fields và ghi file theo dạng stream
    orig_lp = pl.scan_parquet(INPUT_FILE)
    (
        orig_lp.join(final_ids_lp, on="comment_id", how="semi").sink_parquet(
            OUTPUT_FILE, row_group_size=BATCH_SIZE
        )
    )

    print(f"Done! Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    run_pipeline()
