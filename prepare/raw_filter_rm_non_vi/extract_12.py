import regex
import pycld2 as cld2
import os
import polars as pl


def script_filter(text):
    if not text or not text.strip():
        return "empty", 0

    # Loại bỏ các ký tự không phải chữ cái/số (khoảng trắng, icon, dấu câu)
    clean_text = "".join(regex.findall(r"\p{L}|\p{N}", text))
    if not clean_text:
        return "non_language", 0

    total_len = len(clean_text)

    latin_chars = regex.findall(r"\p{Script=Latin}", clean_text)

    latin_ratio = (len(latin_chars) / total_len) * 100

    if latin_ratio > 70:
        return "LATIN_GROUP", latin_ratio
    else:
        return "NON_LATIN_GROUP", latin_ratio


def detect_language_cld2(text):
    if not text or len(str(text).strip()) < 3:
        return "SHORT", 0

    try:
        # Loại bỏ các ký tự rác, emoji để CLD2 tập trung vào chữ
        clean_text = re.sub(r"[^\w\s]", "", str(text))

        # detect() trả về: (isReliable, textBytesFound, details)
        # details là list các tuple: (name, code, percent, score)
        isReliable, textBytesFound, details = cld2.detect(clean_text)

        # Lấy ngôn ngữ top 1
        top_lang = details[0]
        lang_name = top_lang[0]  # 'VIETNAMESE'
        lang_code = top_lang[1]  # 'vi'
        percent = top_lang[2]  # Tỷ lệ % ngôn ngữ này trong câu

        # Logic lọc cho Bước 2:
        if lang_code == "vi":
            return "KEEP_VI", percent

        # Nếu không phải tiếng Việt nhưng CLD2 không chắc chắn (isReliable = False)
        # Hoặc tỷ lệ % thấp -> Có thể là teencode/không dấu mà Google chưa dám khẳng định
        if not isReliable:
            return "POTENTIAL_TEENCODE", percent

        return "FOREIGN", percent

    except Exception:
        return "ERROR", 0


if __name__ == "__main__":

    lazy_df = pl.scan_csv("non_vi_comments_glotlid.csv")

    total_rows = lazy_df.select(pl.len()).collect().item()

    for offset in range(0, total_rows, 500_000):
        chunk = (
            lazy_df.slice(offset, 500_000)
            .select(["comment_id", "comment"])
            .filter(pl.col("comment").is_not_null())
            .collect()
        )

        if chunk.height == 0:
            continue

        texts = chunk["comment"].to_list()
        results = [script_filter(t) for t in texts]

        latin_chunk = chunk.filter(pl.Series([r[0] == "LATIN_GROUP" for r in results]))

        latin_texts = latin_chunk["comment"].to_list()
        cld2_results = [detect_language_cld2(t) for t in latin_texts]
        final_latin_chunk = latin_chunk.filter(
            pl.Series([r[0] in ("KEEP_VI", "POTENTIAL_TEENCODE") for r in cld2_results])
        )

        with open("latin_comments_12_glotlid.csv", mode="ab") as f:
            final_latin_chunk.select(["comment_id", "comment"]).write_csv(
                f, include_header=(f.tell() == 0)
            )

        print(f"Processed batch: {offset:,} / {total_rows:,}")

    print("Done!")
