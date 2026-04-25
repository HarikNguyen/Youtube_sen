import os
import regex
import polars as pl

# Cấu hình file
INPUT_FILE = "raw_filtered_comments.parquet"
OUTPUT_FILE_PARQUET = "cleaned_deduplicated_comments.parquet"
OUTPUT_FILE_CSV = "dedup.csv"

###########################################################################################
####### 1. NHẬN DIỆN ASCII ART & RÁC (DETECT THEN REMOVE) #################################
###########################################################################################


def detect_junk_udf(s: pl.Series) -> pl.Series:
    results = []
    for text in s.to_list():
        t = str(text)
        if not t or len(t) < 2:
            results.append({"is_junk": True})
            continue

        # Tỷ lệ chữ cái/số
        alpha_chars = regex.findall(r"\p{L}|\p{N}", t)
        alpha_ratio = len(alpha_chars) / len(t)

        # Ký tự đồ họa (Box drawing/Blocks)
        has_blocks = bool(regex.search(r"[\u2500-\u25FF]", t))

        # Số dòng
        line_count = t.count("\n") + 1

        is_junk = False
        # Logic: Nhiều dòng + ít chữ HOẶC chứa ký tự vẽ tranh HOẶC mật độ chữ cực thấp
        if has_blocks:
            is_junk = True
        elif line_count > 4 and alpha_ratio < 0.4:
            is_junk = True
        elif len(t) > 50 and alpha_ratio < 0.2:
            is_junk = True
        elif any(len(word) > 40 for word in t.split()):  # Keyboard smash
            is_junk = True

        results.append({"is_junk": is_junk})

    return pl.Series(results)


###########################################################################################
####### 2. KHỬ TRÙNG LẶP (DEDUPLICATION) ##################################################
###########################################################################################


def apply_deduplication(df: pl.DataFrame) -> pl.DataFrame:
    # Level 1: Trùng lặp tuyệt đối
    df = df.unique(subset=["comment"])

    # Level 2: Trùng lặp nội dung cốt lõi (Fingerprint)
    # Loại bỏ icon, dấu cách, ký tự lặp để gom nhóm các câu giống nhau về ý nghĩa
    df = (
        df.with_columns(
            fingerprint=pl.col("comment")
            .str.to_lowercase()
            .str.replace_all(r"[^\p{L}\p{N}]", "")  # Chỉ giữ chữ và số
            .str.replace_all(r"(.)\1+", r"\1")  # 'ngonnnn' -> 'ngon'
        )
        .unique(subset=["fingerprint"])
        .drop("fingerprint")
    )
    return df


###########################################################################################
####### 3. THỰC THI VÀ XUẤT 2 FILE ########################################################
###########################################################################################


def run_cleanup_pipeline():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Không tìm thấy file đầu vào: {INPUT_FILE}")
        return

    print(f"🚀 Đang xử lý làm sạch và khử trùng lặp...")

    # Bước 1: Load và lọc rác
    lp = pl.scan_parquet(INPUT_FILE)

    processed_df = (
        lp.with_columns(
            junk_info=pl.col("comment").map_batches(
                detect_junk_udf,
                return_dtype=pl.Struct([pl.Field("is_junk", pl.Boolean)]),
            )
        )
        .unnest("junk_info")
        .filter(pl.col("is_junk") == False)
        .drop("is_junk")
        .collect()  # Chuyển sang DataFrame để thực hiện unique
    )

    # Bước 2: Khử trùng lặp
    final_df = apply_deduplication(processed_df)

    # Bước 3: Xuất file
    print(f"📥 Số lượng sau khi xử lý: {final_df.height:,} dòng.")

    # Xuất Parquet (Dùng cho pipeline tiếp theo, giữ nguyên schema)
    final_df.write_parquet(OUTPUT_FILE_PARQUET)
    print(f"💾 Đã lưu Parquet: {OUTPUT_FILE_PARQUET}")

    # Xuất CSV (Dùng để view/check tay)
    # Lưu ý: CSV có thể gặp lỗi nếu comment chứa dấu phẩy phức tạp,
    # Polars sẽ tự động xử lý quote.
    final_df.write_csv(OUTPUT_FILE_CSV)
    print(f"💾 Đã lưu CSV: {OUTPUT_FILE_CSV}")


if __name__ == "__main__":
    run_cleanup_pipeline()
