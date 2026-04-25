import os
import re
import unicodedata
import regex
import polars as pl
from tqdm.rich import tqdm

# def detect_junk_udf(s: pl.Series) -> pl.Series:
# results = []
# # Thêm \p{M} để tính cả các dấu tiếng Việt là ký tự hợp lệ
# # Thêm các nhóm Emoji để không bị tính là rác
# valid_pattern = regex.compile(r"[\p{L}\p{N}\p{M}\p{Emoji_Presentation}\p{Extended_Pictographic}]")
# junk_pattern = regex.compile(r"[\u2500-\u25FF\u2800-\u28FF]")

# for text in s.to_list():
# if not text:
# results.append(True); continue

# # CHUẨN HÓA: Đưa về dạng NFC để các dấu dính liền vào chữ cái
# t = unicodedata.normalize('NFC', str(text))
# total_len = len(t)

# if total_len < 5: # Bình luận quá ngắn thường là icon, đừng xóa
# results.append(False); continue

# # Đếm ký tự hợp lệ (Chữ, số, dấu, emoji)
# valid_chars = valid_pattern.findall(t)
# valid_ratio = len(valid_chars) / total_len

# # Kiểm tra khối hình vẽ (Art Junk thực sự)
# has_blocks = bool(junk_pattern.search(t))

# # Kiểm tra từ quá dài (Tránh dính icon hoặc spam ký tự)
# # Tăng lên 60-80 vì link URL hoặc một chuỗi emoji có thể dài
# has_long_word = any(len(word) > 60 for word in t.split())

# # LOGIC MỚI:
# is_junk = (
# has_blocks or
# # Chỉ coi là junk nếu tỷ lệ ký tự hợp lệ CỰC THẤP (dưới 25%)
# (total_len > 50 and valid_ratio < 0.25) or
# has_long_word
# )

# results.append(is_junk)
# return pl.Series(results, dtype=pl.Boolean)


def detect_junk_udf(s: pl.Series) -> pl.Series:
    """User Defined Function (UDF) to detect if a given text is junk (rác nghệ thuật)."""
    results = []
    valid_chars_pattern = regex.compile(
        r"[\p{L}\p{N}\p{Emoji_Presentation}\p{Extended_Pictographic}]"
    )
    junk_patterns = regex.compile(
        r"[\u2500-\u25FF\u2800-\u28FF]"
    )  # Box drawing & Braille art

    for text in s.to_list():
        t = str(text).strip()
        if not t or len(t) < 2:
            results.append(False)
            continue

        # Char/Number/Emojis ratio
        valid_chars = valid_chars_pattern.findall(t)
        valid_ratio = len(valid_chars) / len(t) if len(t) > 0 else 0

        # Check art blocks
        has_junk_blocks = bool(junk_patterns.search(t))

        # Check repeated characters (lower entropy)
        unique_chars = set(t)
        unique_ratio = len(unique_chars) / len(t) if len(t) > 0 else 1

        line_count = t.count("\n") + 1

        is_junk = (
            has_junk_blocks
            or
            # valid_ratio too low (less than 30%) And not emojis
            # (valid_ratio < 0.3 and unique_ratio < 0.2) or
            # text too long but it only use little unique chars
            # (len(t) > 20 and unique_ratio < 0.1) or
            # words too long (maybe spam link)
            # any(len(word) > 50 for word in t.split())
            False
        )
        results.append(is_junk)
    return pl.Series(results, dtype=pl.Boolean)


def main():
    lazy_df = pl.scan_parquet("raw_filtered_comments.parquet")
    batch_size = 1_000_000

    total_rows = lazy_df.select(pl.len()).collect().item()

    with tqdm(
        total=total_rows, desc="[bold green]Cleaning Artist Junk[/]", unit="row"
    ) as pbar:
        for offset in range(0, total_rows, batch_size):
            chunk = (
                lazy_df.slice(offset, batch_size)
                .with_columns(is_junk=pl.col("comment").map_batches(detect_junk_udf))
                .filter(pl.col("is_junk") == True)
                .select(["comment_id", "video_id", "comment"])
                .collect()
            )

            with open("artist_junk.csv", "ab") as f:
                chunk.write_csv(f, include_header=(f.tell() == 0))

            pbar.update(min(batch_size, total_rows - offset))


if __name__ == "__main__":
    main()
