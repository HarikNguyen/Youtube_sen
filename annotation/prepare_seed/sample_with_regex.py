import pandas as pd
import re

# 1. Định nghĩa bộ quy tắc (Giữ nguyên hoặc bổ sung thêm từ khóa)
regex_rules = {
    "grief": r"chia buồn|xót xa|mất mát|đau lòng|thương tiếc|quá cố|tang gia|vĩnh biệt|rip",
    "remorse": r"xin lỗi|hối hận|giá như|ân hận|lỗi tại tôi|cắn rứt|hối lỗi",
    "embarrassment": r"ngượng|xấu hổ|đỏ mặt|ngại|mắc cỡ|quê quá|muối mặt",
    "fear": r"sợ|khiếp|hoảng|lo sợ|hãi|rùng mình|đáng sợ|kinh hoàng",
    "nervousness": r"hồi hộp|lo lắng|bồn chồn|run|lo quá|đứng ngồi không yên",
    "relief": r"may quá|nhẹ nhõm|mừng quá|phù|hên quá|may mà|thoát nạn",
    "optimism": r"hy vọng|tin tưởng|cố lên|tương lai|lạc quan|quyết tâm|khởi sắc",
    "pride": r"tự hào|hãnh diện|vinh dự|ngạo nghễ|vẻ vang|vinh quang",
    "surprise": r"bất ngờ|ngạc nhiên|wow|ồ|không ngờ|thật á|ghê vậy|lạ lẫm",
    "gratitude": r"cảm ơn|thanks|cám ơn|biết ơn|tri ân|đa tạ|trân trọng",
    "caring": r"giữ gìn|bảo trọng|cẩn thận|lo cho|thương hại|quan tâm|giúp đỡ",
    "desire": r"muốn|ước|thèm|khao khát|mong muốn|hi vọng sẽ|giá mà",
    "disgust": r"kinh|tởm|gớm|eo ôi|ghê quá|bẩn|ô uế|kinh tởm",
    "anger": r"tức|điên|chửi|khốn|mất dạy|cút|bố láo|phẫn nộ|điên tiết",
    "love": r"yêu|thương|tym|❤️|mê|crush|thích|yêu quý",
    "joy": r"vui|hạnh phúc|tuyệt vời|sướng|haha|hihi|vui vẻ|phấn khởi",
    "amusement": r"buồn cười|hài hước|tấu hài|cười ẻ|vui tính|dí dỏm",
    "admiration": r"ngưỡng mộ|đỉnh|quá giỏi|tuyệt quá|xuất sắc|hâm mộ",
}

# 2. Đọc dữ liệu
print("Đang đọc dữ liệu...")
df = pd.read_parquet('sampled_5852772.parquet')

# 3. Cắt bỏ phần rác [TITLE]...[CHANNEL]...[CATEGORY]
# Chỉ giữ lại từ [COMMENT] trở đi
print("Đang làm sạch văn bản (cắt bỏ metadata)...")
df['text_clean'] = df['text'].str.extract(r'(\[COMMENT\].*)', expand=False).fillna(df['text'])

# 4. Dự đoán nhãn bằng phương thức Vectorized (Nhanh hơn .apply gấp nhiều lần)
print("Đang dự đoán nhãn bằng Regex...")
df['suggested_label'] = "unknown"

# Chuyển sang lowercase một lần duy nhất để tăng tốc
text_lower = df['text_clean'].str.lower()

for label, pattern in regex_rules.items():
    # Tạo mask cho những dòng chưa có nhãn và khớp với pattern
    mask = (df['suggested_label'] == "unknown") & (text_lower.str.contains(pattern, na=False, regex=True))
    df.loc[mask, 'suggested_label'] = label

# 5. Lọc và lưu kết quả
minority_labels = ["grief", "remorse", "embarrassment", "fear", "nervousness", "relief", "optimism", "pride", "surprise"]
candidates = df[df['suggested_label'].isin(minority_labels)]

print(f"Hoàn thành! Tìm thấy {len(candidates)} mẫu tiềm năng cho các nhãn hiếm.")

# Lưu kết quả lọc được để gán nhãn thủ công cho nhanh
candidates.to_csv('minority_candidates_to_label.csv', index=False)

