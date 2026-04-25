import polars as pl
import re
import regex

# --- ĐƯỜNG DẪN FILE ---
CLEANED_FILE = "cleaned_deduplicated_comments.parquet"
DEDUP_FILE = "dedup.csv"


def collapse_characters(s: pl.Series) -> pl.Series:
    pattern = re.compile(r"(.)\1+")
    return pl.Series(
        [pattern.sub(r"\1", str(val)) if val else val for val in s.to_list()]
    )


def validate_process():
    print("🔍 Đang nạp dữ liệu để xác thực...")

    # 1. Load 2 file
    df_clean = pl.read_parquet(CLEANED_FILE)
    df_dedup = pl.read_csv(DEDUP_FILE)

    # 2. Tạo fingerprint cho file Cleaned để đối chiếu
    # (Dùng lại logic fingerprint của bước trước)
    df_clean_fp = df_clean.with_columns(
        fingerprint=pl.col("comment")
        .str.to_lowercase()
        .str.replace_all(r"[^\p{L}\p{N}]", "")
        .map_batches(collapse_characters)
    ).select(["fingerprint"])

    # 3. Xử lý file Dedup để kiểm tra
    df_check = df_dedup.with_columns(
        fingerprint=pl.col("comment")
        .str.to_lowercase()
        .str.replace_all(r"[^\p{L}\p{N}]", "")
        .map_batches(collapse_characters)
    )

    # 4. Kiểm tra sự tồn tại (Join)
    # Những dòng trong dedup mà có fingerprint khớp với file clean
    valid_duplicates = df_check.join(df_clean_fp, on="fingerprint", how="inner")

    # Những dòng trong dedup mà KHÔNG khớp với file clean (Có thể là Junk/Artist)
    potential_junk = df_check.join(df_clean_fp, on="fingerprint", how="anti")

    # 5. Phân tích kết quả
    total_dedup = df_dedup.height
    num_duplicates = valid_duplicates.height
    num_junk = potential_junk.height

    print("\n" + "=" * 50)
    print("📊 KẾT QUẢ XÁC THỰC FILE DEDUP.CSV")
    print("=" * 50)
    print(f"✅ Tổng số dòng trong file dedup: {total_dedup:,}")
    print(
        f"👉 Số dòng là TRÙNG LẶP (đã có bản gốc trong file Clean): {num_duplicates:,} ({(num_duplicates/total_dedup)*100:.2f}%)"
    )
    print(
        f"👉 Số dòng là RÁC/ARTIST (không có bản gốc, bị lọc bỏ):  {num_junk:,} ({(num_junk/total_dedup)*100:.2f}%)"
    )

    if num_junk > 0:
        print("\n📝 Ví dụ 5 mẫu được coi là RÁC/ARTIST (Junk):")
        # In ra 5 mẫu không có trong file clean để bạn check mắt
        sample_junk = potential_junk.select("comment").head(5).to_series().to_list()
        for i, msg in enumerate(sample_junk):
            print(f"   [{i}] {repr(msg)[:100]}...")

    print("=" * 50)


if __name__ == "__main__":
    validate_process()
