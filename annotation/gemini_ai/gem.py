import google.generativeai as genai
import pandas as pd
import polars as pl
import json
import time
from datetime import datetime
import io
import os

INPUT_FILE = "sampled_10000.parquet"
OUTPUT_FILE = "pre_labeled_10000.csv"
BATCH_SIZE = 40

API_KEY = "<google_ai_studio_api_here>"
genai.configure(api_key=API_KEY)

SYSTEM_PROMPT = """
Bạn là một chuyên gia phân tích ngôn ngữ và cảm xúc trên nền tảng YouTube tại Việt Nam.
Nhiệm vụ: Gán DUY NHẤT một nhãn cảm xúc từ danh sách dưới đây cho mỗi dòng dữ kiện được điều chỉnh gồm:
    - [TITLE] <video title> [CHANNEL] <channel name> [CATEGORY] <category name> [COMMENT] <comment> (Nếu comment này parent comment)
    - [TITLE] <video title> [CHANNEL] <channel name> [CATEGORY] <category name> [COMMENT] <comment> [REPLY] <reply> (Nếu comment này reply cho parent comment)

DANH SÁCH NHÃN:
[admiration, amusement, anger, annoyance, approval, caring, confusion, curiosity, desire, disappointment, disapproval, disgust, embarrassment, excitement, fear, gratitude, grief, joy, love, nervousness, optimism, pride, realization, relief, remorse, sadness, surprise, neutral]

CÁC NHÃN ĐƯỢC ĐỊNH NGHĨA NHƯ SAU:
    - admiration - Finding something impressive or worthy of respect.
    - amusement - Finding something funny or being entertained.
    - anger - A strong feeling of displeasure or antagonism.
    - annoyance - Mild anger, irritation.
    - approval - Having or expressing a favorable opinion.
    - caring - Displaying kindness and concern for others.
    - confusion - Lack of understanding, uncertainty.
    - curiosity - A strong desire to know or learn something.
    - desire - A strong feeling of wanting something or wishing for something to happen.
    - disappointment - Sadness or displeasure caused by the nonfulfillment of one’s hopes or expectations.
    - disapproval - Having or expressing an unfavorable opinion.
    - disgust - Revulsion or strong disapproval aroused by something unpleasant or offensive.
    - embarrassment - Self-consciousness, shame, or awkwardness.
    - excitement - Feeling of great enthusiasm and eagerness.
    - fear - Being afraid or worried.
    - gratitude - A feeling of thankfulness and appreciation.
    - grief - Intense sorrow, especially caused by someone’s death.
    - joy - A feeling of pleasure and happiness.
    - love - A strong positive emotion of regard and affection.
    - nervousness - Apprehension, worry, anxiety.
    - optimism - Hopefulness and confidence about the future or the success of something.
    - pride - Pleasure or satisfaction due to ones own achievements or the achievements of those with whom one is closely associated.
    - realization - Becoming aware of something.
    - relief - Reassurance and relaxation following release from anxiety or distress.
    - remorse - Regret or guilty feeling.
    - sadness - Emotional pain, sorrow.
    - surprise - Feeling astonished, startled by something unexpected.
    - neutral - Neutral of emotion.

QUY TẮC PHÂN TÍCH:
1. Đối tượng hướng đến: Phản ứng của người comment (dù có là parent hay là child) đều là đối với nội dung VIDEO (Title/Channel/Category - video's metadata).
2. Đối với trường hợp có REPLY: Dựa vào thông tin từ parent comment để xác định cảm xúc của replier đối với parent comment thông qua ngữ cảnh của video (video's metadata).
3. Ngôn ngữ: Hiểu các từ viết tắt, slang, emoji (ví dụ: "luỵ" -> sadness, "hóng" -> excitement, "clgt" -> confusion).
4. Định dạng đầu ra: BẮT BUỘC TRẢ VỀ JSON ARRAY gồm các object có chứa các trường sau và KHÔNG GIẢI THÍCH GÌ THÊM:
    - text
    - labels
    - reason - là cơ sở lập luận ngắn gọn của bạn tại sao dữ kiện này lại được đánh nhãn này (ĐƯỢC TRẢ LỜI BẰNG TIẾNG VIỆT)!

MỘT VÀI VÍ DỤ:
- Ví dụ 1:
+ text: TITLE TỪ UKRAINE ĐẾN BURKINA FASO - TRIỀU TIÊN LIÊN TỤC ĐƯA QUÂN VIỄN CHINH VÀ XÁC LẬP TẦM ẢNH HƯỞNG CHANNEL BATTLECRY - LỊCH SỬ THẾ GIỚI CATEGORY news COMMENT Coi như lực lượng đánh thuê chứ Triều có xuất khẩu gì đâu. Đổi dc dầu và vàng về kiến thiết cũng cần mà.
+ labels: realization
+ reason: Người xem nhận ra bản chất thực tế của sự việc (xuất khẩu quân đội đổi lấy tài nguyên) thay vì chỉ nhìn ở góc độ quân sự đơn thuần.

- Ví dụ 2:
+ text: TITLE Theo Bạn Từ Nào Mà Ai Phát Âm Cũng Sai | La Yen Team | #Shorts CHANNEL La Yen Team CATEGORY entertainment COMMENT Câu 1:""sai"" Câu 2: 4:3 là bằng tứ chia tam mà tứ chia tam bằng tám :tư= 8:4=2 REPLY Về
+ labels: annoyance
+ reason: (Xét cảm xúc của Replier) Một phản ứng cụt ngủn, có vẻ khó chịu hoặc muốn chấm dứt cuộc đối thoại sau khi đọc câu trả lời của Comment.

- Ví dụ 3:
+ text: TITLE Giọng Ải Giọng Ai 4| Tập 2 full: Thỏ đen Ngô Kiến Huy “chết đứng” khi được gái đẹp BẤT NGỜ TỎ TÌNH CHANNEL DIEN QUAN Entertainment / Giải Trí CATEGORY entertainment COMMENT chị Chanh để mic xa vậy vẫn hát được ghê thiệt
+ labels: admiration
+ reason: Thể hiện sự ấn tượng trước kỹ thuật hát và giọng hát nội lực của ca sĩ Phương Thanh.
"""


def analyze_emotions(texts):
    model = genai.GenerativeModel(
        model_name="gemini-3.1-flash-lite-preview", system_instruction=SYSTEM_PROMPT
    )

    text_flats = "\n".join(" - " + text for text in texts)
    while True:

        try:
            response = model.generate_content(
                f"=======\nPhân tích các câu sau:\n{text_flats}",
                generation_config={"response_mime_type": "application/json"},
            )

            # Parse to JSON
            batch_json = json.loads(response.text)
            print("COMPLETED batch at ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            time.sleep(4)
            return batch_json

        except Exception as e:
            print(f"ERR: {e}")
            time.sleep(10)

def main():
    lf = pl.scan_parquet(INPUT_FILE)
    total = lf.select(pl.len()).collect().item()

    for i in range(0, total, BATCH_SIZE):
        batch = lf.slice(i, BATCH_SIZE).collect(engine="streaming")
        batch_text = batch["text"].to_list()
        batch_comment_id = batch["comment_id"].to_list()

        results_json = analyze_emotions(batch_text)

        df_comment_id = pd.DataFrame({"comment_id": batch_comment_id})
        df_batch = pd.DataFrame(results_json)
        # add comment_id
        df_batch = pd.concat([df_comment_id, df_batch], axis=1)

        header = not os.path.exists(OUTPUT_FILE)
        df_batch.to_csv(
            OUTPUT_FILE, mode="a", index=False, header=header, encoding="utf-8-sig"
        )


if __name__ == "__main__":
    main()
