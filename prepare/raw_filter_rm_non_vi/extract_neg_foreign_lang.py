import fasttext
import polars as pl
import re
import os

# --- CẤU HÌNH ---
MODEL_PATH = "lid.176.bin"
INPUT_FILE = "latin_comments.csv"
OUTPUT_VI = "potential_vi_comments.csv"
OUTPUT_FR = "fr_comments.csv"

# Kiểm tra file model
if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(f"Vui lòng tải model lid.176.bin và để vào: {MODEL_PATH}")

# Tải model FastText
model = fasttext.load_model(MODEL_PATH)

# Danh sách mở rộng các ngôn ngữ Latin chính thống
SAFE_TO_REMOVE = [
    "en",
    "fr",
    "es",
    "de",
    "it",
    "pt",  # Nhóm chính
    "nl",
    "sv",
    "da",
    "no",
    "fi",  # Bắc Âu & Hà Lan
    "pl",
    "cs",
    "sk",
    "hu",
    "ro",  # Đông Âu
    "tr",
    "af",
    "sw",  # Thổ Nhĩ Kỳ, Afrikaans, Swahili
]

DANGER_ZONE = ["id", "ms", "tl"]  # Indonesia, Malay, Tagalog


def neg_filter_v2(texts):

    if not texts:
        return []

    clean_texts = [str(t).replace("\n", " ").replace("\r", " ").strip() for t in texts]
    labels, probs = model.predict(clean_texts, k=1)

    return [
        (
            False
            if label[0].replace("__label__", "") == "vi"
            else (
                True
                if label[0].replace("__label__", "") in SAFE_TO_REMOVE and prob[0] > 0.5
                else (
                    True
                    if label[0].replace("__label__", "") in DANGER_ZONE
                    and prob[0] > 0.9
                    else False
                )
            )
        )
        for label, prob in zip(labels, probs)
    ]


if __name__ == "__main__":
    lazy_df = pl.scan_csv(INPUT_FILE)
    total_rows = lazy_df.select(pl.len()).collect().item()
    print(f"Bắt đầu xử lý {total_rows} dòng...")

    batch_size = 500_000
    first_run = True

    for offset in range(0, total_rows, batch_size):
        chunk = (
            lazy_df.slice(offset, batch_size)
            .filter(pl.col("comment").is_not_null())
            .collect()
        )

        if chunk.height == 0:
            continue

        # Dự đoán
        texts = chunk["comment"].to_list()
        is_foreign_flags = neg_filter_v2(texts)
        chunk = chunk.with_columns(pl.Series("is_foreign", is_foreign_flags))

        # --- TÁCH DỮ LIỆU ---
        # 1. Potential VI (is_foreign == False)
        vi_chunk = chunk.filter(pl.col("is_foreign") == False).drop("is_foreign")

        # 2. Foreign (is_foreign == True)
        fr_chunk = chunk.filter(pl.col("is_foreign") == True).drop("is_foreign")

        # --- GHI FILE ---
        if first_run:
            # Ghi mới và tạo header cho cả 2 file
            if vi_chunk.height > 0:
                vi_chunk.write_csv(OUTPUT_VI)
            if fr_chunk.height > 0:
                fr_chunk.write_csv(OUTPUT_FR)
            first_run = False
        else:
            # Ghi nối tiếp (Append) không kèm header
            if vi_chunk.height > 0:
                with open(OUTPUT_VI, mode="ab") as f:
                    vi_chunk.write_csv(f, include_header=False)

            if fr_chunk.height > 0:
                with open(OUTPUT_FR, mode="ab") as f:
                    fr_chunk.write_csv(f, include_header=False)

        processed = min(offset + batch_size, total_rows)
        print(f"Đã xử lý: {processed}/{total_rows} dòng...")

    print(f"--- HOÀN THÀNH ---")
    print(f"1. File Việt: {OUTPUT_VI}")
    print(f"2. File Ngoại: {OUTPUT_FR}")
