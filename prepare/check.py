import polars as pl
import regex
from collections import Counter

DEDUP_FILE = "dedup.csv"


def verify_dedup_log():
    if not os.path.exists(DEDUP_FILE):
        print(f"❌ Không tìm thấy file {DEDUP_FILE}")
        return

    # 1. Load dữ liệu trùng lặp
    df = pl.read_csv(DEDUP_FILE)
    total = len(df)

    print(f"🧐 Đang phân tích {total:,} mẫu trong log trùng lặp...\n")

    # 2. Phân tích loại "Rác" (Artist/Junk)
    def classify_junk(text):
        t = str(text)
        # Check ký tự đồ họa
        if regex.search(r"[\u2500-\u25FF]", t):
            return "ASCII_Art (Blocks)"
        # Check mật độ chữ cái
        alpha_len = len(regex.findall(r"\p{L}|\p{N}", t))
        ratio = alpha_len / len(t) if len(t) > 0 else 0
        if ratio < 0.3:
            return "ASCII_Art (Symbols)"
        if any(len(word) > 40 for word in t.split()):
            return "Keyboard_Smash"
        return "Duplicate_Content"

    df = df.with_columns(
        category=pl.col("comment").map_elements(classify_junk, return_dtype=pl.String)
    )

    # 3. Thống kê tỷ lệ
    stats = (
        df.group_by("category")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )

    print("📊 BÁO CÁO PHÂN LOẠI LOG:")
    print("-" * 40)
    for row in stats.to_dicts():
        percentage = (row["count"] / total) * 100
        print(f"🔹 {row['category']:<20}: {row['count']:>8,} mẫu ({percentage:>6.2f}%)")
    print("-" * 40)

    # 4. Hiển thị mẫu ngẫu nhiên để bạn xác thực bằng mắt
    print("\n🔍 XEM MẪU NGẪU NHIÊN THEO TỪNG LOẠI:")
    for cat in df["category"].unique():
        print(f"\n--- [ {cat} ] ---")
        samples = (
            df.filter(pl.col("category") == cat)
            .sample(min(3, total))
            .select("comment")
            .to_series()
            .to_list()
        )
        for i, s in enumerate(samples):
            # Cắt ngắn nếu comment quá dài
            display_text = s[:150].replace("\n", " ") + "..." if len(s) > 150 else s
            print(f"  {i+1}. {display_text}")

    # 5. Kiểm tra "Duplicate_Content" (Fingerprint nội bộ)
    # Xem trong file dedup có bao nhiêu nội dung thực sự giống nhau
    top_dups = (
        df.filter(pl.col("category") == "Duplicate_Content")
        .group_by("comment")
        .len()
        .sort("len", descending=True)
        .head(5)
    )

    if len(top_dups) > 0:
        print("\n📢 TOP 5 NỘI DUNG TRÙNG LẶP NHIỀU NHẤT:")
        for row in top_dups.to_dicts():
            print(f"  [{row['len']:,} lần]: {row['comment'][:100]}")


if __name__ == "__main__":
    import os

    verify_dedup_log()
