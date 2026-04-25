import os
import regex
import polars as pl
from tqdm import tqdm

# --- CẤU HÌNH ---
INPUT_FILE = "raw_filtered_comments.parquet"
OUTPUT_CLEAN = "cleaned_deduplicated_comments.parquet"
OUTPUT_DEDUP_LOG = "dedup.csv"
BATCH_SIZE = 1_000_000

###########################################################################################
####### 1. HÀM NHẬN DIỆN JUNK (ASCII ART, SPAM) ###########################################
###########################################################################################


def detect_junk_udf(s: pl.Series) -> pl.Series:
    results = []
    for text in s.to_list():
        t = str(text)
        if not t or len(t) < 2:
            results.append(True)
            continue

        alpha_chars = regex.findall(r"\p{L}|\p{N}", t)
        alpha_ratio = len(alpha_chars) / len(t) if len(t) > 0 else 0
        has_blocks = bool(regex.search(r"[\u2500-\u25FF]", t))
        line_count = t.count("\n") + 1

        is_junk = (
            has_blocks
            or (line_count > 4 and alpha_ratio < 0.4)
            or (len(t) > 50 and alpha_ratio < 0.2)
            or any(len(word) > 40 for word in t.split())
        )
        results.append(is_junk)
    return pl.Series(results, dtype=pl.Boolean)


###########################################################################################
####### 2. LUỒNG XỬ LÝ CHÍNH ##############################################################
###########################################################################################


def run_cleanup_step_2():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Thiếu file: {INPUT_FILE}")
        return

    lazy_df = pl.scan_parquet(INPUT_FILE)
    total_rows = lazy_df.select(pl.len()).collect().item()

    print(f"🚀 Đang xử lý {total_rows:,} comments...")
    collected_chunks = []

    # BƯỚC A: LỌC JUNK THEO BATCH
    with tqdm(
        total=total_rows, desc="🧹 Cleaning Junk", unit="row", colour="green"
    ) as pbar:
        for offset in range(0, total_rows, BATCH_SIZE):
            chunk = lazy_df.slice(offset, BATCH_SIZE).collect()

            # Chỉ loại bỏ Junk (tranh ASCII, rác nghệ thuật) ở bước này
            clean_chunk = (
                chunk.with_columns(
                    is_junk=pl.col("comment").map_batches(detect_junk_udf)
                )
                .filter(pl.col("is_junk") == False)
                .drop("is_junk")
            )
            collected_chunks.append(clean_chunk)
            pbar.update(len(chunk))

    # Hợp nhất dữ liệu sau khi lọc junk
    df_all = pl.concat(collected_chunks)
    del collected_chunks  # Giải phóng RAM

    print("\n🔗 Đang phân tích trùng lặp...")

    # BƯỚC B: TRÍCH XUẤT TRÙNG LẶP (DEDUPLICATION LOG)
    # 1. Trùng lặp chính xác (Exact match)
    # Lấy ra những dòng mà comment bị lặp lại (giữ lại từ bản thứ 2 trở đi)
    exact_duplicates = df_all.filter(pl.col("comment").is_duplicated())

    # 2. Trùng lặp cấp độ câu (Fingerprint)
    # Tạo vân tay để lọc các biến thể (ngonnn vs ngon)
    df_with_fp = df_all.with_columns(
        fingerprint=pl.col("comment")
        .str.to_lowercase()
        .str.replace_all(r"[^\p{L}\p{N}]", "")
        .str.replace_all(r"(.)\1+", r"\1")
    )

    # Lấy ra các dòng có fingerprint trùng nhưng comment khác nhau (hoặc giống nhau)
    fp_duplicates = df_with_fp.filter(pl.col("fingerprint").is_duplicated())

    # BƯỚC C: XUẤT FILE TRÙNG LẶP (DEDUP.CSV)
    # Gộp 2 loại trùng lặp lại để lưu log
    dedup_log = pl.concat(
        [
            exact_duplicates.select(["comment_id", "comment"]),
            fp_duplicates.select(["comment_id", "comment"]),
        ]
    ).unique(
        subset=["comment_id"]
    )  # Tránh ghi trùng ID trong log

    print(f"💾 Đang ghi danh sách trùng lặp vào: {OUTPUT_DEDUP_LOG}")
    dedup_log.write_csv(OUTPUT_DEDUP_LOG)

    # BƯỚC D: XUẤT FILE SẠCH (PARQUET)
    # Loại bỏ trùng lặp để lấy dữ liệu cuối cùng
    final_df = df_with_fp.unique(subset=["fingerprint"]).drop("fingerprint")

    print(f"💾 Đang ghi dữ liệu sạch vào: {OUTPUT_CLEAN}")
    final_df.write_parquet(OUTPUT_CLEAN)

    # Báo cáo
    print(f"\n✅ Hoàn thành!")
    print(f"📊 Kết quả:")
    print(f"   - Tổng mẫu sạch: {final_df.height:,} -> {OUTPUT_CLEAN}")
    print(f"   - Tổng mẫu trùng: {dedup_log.height:,} -> {OUTPUT_DEDUP_LOG}")


if __name__ == "__main__":
    run_cleanup_step_2()
