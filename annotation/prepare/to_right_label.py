import pandas as pd

label_mapping = {
    # --- 28 Nhãn chính (Giữ nguyên) ---
    "admiration": "admiration",
    "amusement": "amusement",
    "anger": "anger",
    "annoyance": "annoyance",
    "approval": "approval",
    "caring": "caring",
    "confusion": "confusion",
    "curiosity": "curiosity",
    "desire": "desire",
    "disappointment": "disappointment",
    "disapproval": "disapproval",
    "disgust": "disgust",
    "embarrassment": "embarrassment",
    "excitement": "excitement",
    "fear": "fear",
    "gratitude": "gratitude",
    "grief": "grief",
    "joy": "joy",
    "love": "love",
    "nervousness": "nervousness",
    "optimism": "optimism",
    "pride": "pride",
    "realization": "realization",
    "relief": "relief",
    "remorse": "remorse",
    "sadness": "sadness",
    "surprise": "surprise",
    "neutral": "neutral",
    # --- Điều chỉnh các nhãn phụ về 28 nhãn chính ---
    "nostalgia": "sadness",  # Hoài niệm thường mang sắc thái buồn nhẹ
    "correction": "neutral",  # Sửa lỗi thường mang tính khách quan
    "disbelief": "surprise",  # Không tin nổi -> Ngạc nhiên
    "dislike": "disapproval",  # Không thích -> Phản đối
    "sarcasm": "annoyance",  # Mỉa mai -> Khó chịu/Giận dữ
    "caution": "fear",  # Thận trọng -> Lo sợ
    "skepticism": "confusion",  # Hoài nghi -> Bối rối/Chưa hiểu rõ
    "cynicism": "disapproval",  # Hoài nghi tiêu cực -> Phản đối
    "suspicion": "confusion",  # Nghi ngờ -> Bối rối
    "disrespect": "disapproval",  # Thiếu tôn trọng -> Phản đối
    "pessimism": "sadness",  # Bi quan -> Buồn
    "empathy": "caring",  # Đồng cảm -> Quan tâm
    "mockery": "disapproval",  # Chế nhạo -> Phản đối
    "defensiveness": "annoyance",  # Tự vệ -> Khó chịu
    "anticipation": "optimism",  # Mong đợi -> Lạc quan/Hào hứng
    "discomfort": "fear",  # Khó ở -> Lo lắng/Sợ
    "enthusiasm": "excitement",  # Nhiệt huyết -> Hào hứng
    "observation": "neutral",  # Quan sát -> Trung lập
    "reassurance": "caring",  # Trấn an -> Quan tâm
    "disagreement": "disapproval",  # Không đồng ý -> Phản đối
    "agreement": "approval",  # Đồng ý -> Tán thành
}

df = pd.read_csv("ft_10000.csv")
df["labels"] = df["labels"].map(label_mapping)

labels_stats = df["labels"].value_counts()
labels_perc = df["labels"].value_counts(normalize=True) * 100

summary = pd.DataFrame(
    {
        "label": labels_stats.index,
        "count": labels_stats.values,
        "percentage": labels_perc.values,
    }
)
summary = summary.sort_values(by="count", ascending=False)

print(summary)

df.to_csv("ft_10000_right_label.csv", index=False)
