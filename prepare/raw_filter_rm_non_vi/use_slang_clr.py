import polars as pl
import fasttext

# 1. Tải mô hình
model = fasttext.load_model("my_slang_classifier.bin")


# Hàm dự đoán batch (giữ nguyên)
def predict_batch(s: pl.Series) -> pl.Series:
    texts = s.fill_null("").to_list()
    labels, probs = model.predict(texts)
    results = [
        {"pred_label": lbl[0].replace("__label__", ""), "confidence": float(p[0])}
        for lbl, p in zip(labels, probs)
    ]
    return pl.Series(results)


# 2. Thiết lập cấu hình và Pipeline
input_file = "latin_comments_glotlid.csv"
output_file = "test_02.csv"
vietnamese_chars = (
    "a-z0-9áàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđ"
)

print(f"Đang xử lý dữ liệu từ {input_file}...")

lazy_df = (
    pl.scan_csv(input_file)
    # BƯỚC A: Làm sạch text để dự đoán chính xác hơn
    .with_columns(
        pl.col("comment")
        .str.to_lowercase()
        .str.replace_all(r"[\n\r]", " ")
        .str.replace_all(f"[^{vietnamese_chars}]", " ")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .alias("cleaned_comment")
    )
    # BƯỚC B: Dự đoán
    .with_columns(
        pl.col("cleaned_comment")
        .map_batches(
            predict_batch,
            return_dtype=pl.Struct(
                [pl.Field("pred_label", pl.String), pl.Field("confidence", pl.Float64)]
            ),
        )
        .alias("prediction")
    ).unnest("prediction")
    # ---------------------------------------------------
    # BƯỚC C: LỌC VÀ CHỌN CỘT (Theo yêu cầu mới)
    # ---------------------------------------------------
    # 1. Chỉ lấy những comment được dự đoán là tiếng Việt ('vi')
    .filter(pl.col("pred_label") == "vi")
    # 2. Chỉ giữ lại 3 cột cần thiết
    .select(["comment_id", "comment", "pred_label"])
)

# 3. Thực thi và lưu kết quả
# Sử dụng sink_csv để đảm bảo hiệu năng streaming cho file lớn
print(f"Đang ghi kết quả đã lọc vào {output_file}...")
lazy_df.sink_csv(output_file)

print("Xong! Đã lọc và lưu danh sách comment tiếng Việt.")
