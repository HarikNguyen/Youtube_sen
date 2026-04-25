import os
import regex
import polars as pl
from tqdm import tqdm

# --- CẤU HÌNH ---
INPUT_FILE = "raw_filtered_comments.parquet"
OUTPUT_FILE = "cleaned_deduplicated_comments.parquet"
BATCH_SIZE = 250_000  # Tăng giảm tùy theo RAM (250k là mức an toàn)

###########################################################################################
####### 1. HÀM NHẬN DIỆN JUNK (ASCII ART, SPAM) ###########################################
###########################################################################################


def detect_junk_udf(s: pl.Series) -> pl.Series:
    """
    Hàm nhận diện rác nghệ thuật.
    Lưu ý: s ở đây là 1 Series (dữ liệu thật) do được gọi qua .map_batches()
    """
    results = []
    for text in s.to_list():
        t = str(text)
        if not t or len(t) < 2:
            results.append(True)
            continue

        # 1. Tỷ lệ chữ cái/số (Alpha Ratio)
        # Slang như 'vcl', 'đcm' sẽ có ratio = 1.0 nên cực kỳ an toàn
        alpha_chars = regex.findall(r"\p{L}|\p{N}", t)
        alpha_ratio = len(alpha_chars) / len(t) if len(t) > 0 else 0

        # 2. Kiểm tra ký tự đồ họa khối (█, ▓, ▒, ░, ─, │...)
        has_blocks = bool(regex.search(r"[\u2500-\u25FF]", t))

        # 3. Đếm số dòng
        line_count = t.count("\n") + 1

        # LOGIC QUYẾT ĐỊNH JUNK
        is_junk = (
            has_blocks
            or (line_count > 4 and alpha_ratio < 0.4)
            or (len(t) > 50 and alpha_ratio < 0.2)
            or any(len(word) > 40 for word in t.split())  # Keyboard smash
        )
        results.append(is_junk)

    return pl.Series(results, dtype=pl.Boolean)


###########################################################################################
####### 2. HÀM XỬ LÝ CHÍNH VỚI THANH TIẾN ĐỘ #############################################
###########################################################################################


def run_cleanup_step_2():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Không tìm thấy file đầu vào: {INPUT_FILE}")
        return

    # Quét metadata lấy tổng số dòng
    lazy_df = pl.scan_parquet(INPUT_FILE)
    total_rows = lazy_df.select(pl.len()).collect().item()

    print(f"🚀 Đang xử lý {total_rows:,} comments...")

    collected_chunks = []

    # Khởi tạo thanh tiến độ tqdm
    with tqdm(
        total=total_rows, desc="🧹 Cleaning Junk", unit="row", colour="green"
    ) as pbar:
        for offset in range(0, total_rows, BATCH_SIZE):
            # 1. Trích xuất một batch dữ liệu
            chunk = lazy_df.slice(offset, BATCH_SIZE).collect()

            # 2. Áp dụng bộ lọc Junk (Sửa lỗi map_batches ở đây)
            clean_chunk = (
                chunk.with_columns(
                    is_junk=pl.col("comment").map_batches(detect_junk_udf)
                )
                .filter(pl.col("is_junk") == False)
                .drop("is_junk")
            )

            collected_chunks.append(clean_chunk)

            # Cập nhật thanh tiến độ
            pbar.update(len(chunk))

    # --- BƯỚC 3: DEDUPLICATION (KHỬ TRÙNG LẶP TOÀN CỤC) ---
    print("\n🔗 Đang hợp nhất các batch và khử trùng lặp 2 cấp độ...")

    # Gộp tất cả các chunks đã sạch junk
    df_all = pl.concat(collected_chunks)

    # Level 1: Trùng lặp chính xác (Exact Match)
    df_all = df_all.unique(subset=["comment"])

    # Level 2: Trùng lặp nội dung cốt lõi (Fingerprinting)
    print("✨ Đang tính toán Fingerprint để lọc biến thể...")
    final_df = (
        df_all.with_columns(
            fingerprint=pl.col("comment")
            .str.to_lowercase()
            .str.replace_all(r"[^\p{L}\p{N}]", "")  # Loại bỏ icon/dấu câu/khoảng trắng
            .str.replace_all(r"(.)\1+", r"\1")  # 'ngonnnnn' -> 'ngon'
        )
        .unique(subset=["fingerprint"])
        .drop("fingerprint")
    )

    # 4. Ghi file kết quả
    print(f"💾 Đang ghi dữ liệu xuống: {OUTPUT_FILE}")
    final_df.write_parquet(OUTPUT_FILE)

    # Báo cáo kết quả
    print(f"\n✅ Hoàn thành rực rỡ!")
    print(f"📊 Thống kê:")
    print(f"   - Đầu vào: {total_rows:,} dòng")
    print(f"   - Đầu ra:  {final_df.height:,} dòng")
    print(f"   - Loại bỏ: {total_rows - final_df.height:,} dòng (Junk & Duplicates)")


if __name__ == "__main__":
    run_pipeline = run_cleanup_step_2()
