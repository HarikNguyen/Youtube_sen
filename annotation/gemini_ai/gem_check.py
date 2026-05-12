from google import genai
from google.genai import types
import pandas as pd
import polars as pl
import json
import time
from datetime import datetime, timedelta
import io
import os

INPUT_FILE = "<file_to_check>.parquet"
OUTPUT_FILE = "pre_<file_to_out>.csv"
API_BATCH_SIZE = 35
MAX_WORKERS = 4
BATCH_SIZE = API_BATCH_SIZE * MAX_WORKER

API_KEY = "<google_ai_studio_api_here>"
client = genai.Client(api_key=API_KEY)

SYSTEM_PROMPT = """
Bạn là một chuyên gia phân tích ngôn ngữ và cảm xúc trên nền tảng YouTube tại Việt Nam.
Nhiệm vụ: Gán DUY NHẤT một nhãn cảm xúc từ danh sách dưới đây cho mỗi dòng dữ kiện được cung cấp.
Bạn là một chuyên gia kiểm định (Reviewer/Judge) phân tích ngôn ngữ và cảm xúc trên YouTube tại Việt Nam.
Nhiệm vụ: Bạn sẽ nhận được các dòng dữ liệu gồm một đoạn ngữ liệu (ngữ cảnh video và comment) và một "Nhãn dự đoán ban đầu" (labels) cho bài toán phân tích cảm xúc gồm 28 cảm xúc. Hãy ĐÁNH GIÁ LẠI một cách khách quan xem labels đã chính xác với bản chất của ngữ liệu chưa. 
- Nếu ĐÚNG: Hãy giữ nguyên nhãn đó.
- Nếu SAI: Hãy CHỌN LẠI một nhãn chính xác nhất từ danh sách để sửa lỗi.

CẤU TRÚC NGỮ LIỆU NHƯ SAU:
    - [TITLE] <video title> [CHANNEL] <channel name> [CATEGORY] <category name> [COMMENT] <comment> (Nếu comment này parent comment) [IN_YEAR] <year>
    - [TITLE] <video title> [CHANNEL] <channel name> [CATEGORY] <category name> [COMMENT] <comment> [REPLY] <reply> (Nếu comment này reply cho parent comment) [IN_YEAR] <year>

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

QUY TẮC PHÂN TÍCH VÀ SỬA LỖI:
1. Đừng tin tưởng mù quáng vào labels: Hãy suy luận độc lập. Tập dữ liệu hiện tại đang bị dự đoán sai rất nhiều (đặc biệt là hay gán nhầm thành grief, fear dù câu đó rất bình thường). Nếu câu không thể hiện cảm xúc rõ rệt, hãy mạnh dạn đổi thành 'neutral'.
2. Xét ngữ cảnh chặt chẽ: Phản ứng của người comment là đối với nội dung VIDEO (Title/Channel). Nếu là REPLY, thì đó là cảm xúc của replier dành cho parent comment.
3. Định dạng đầu ra: BẮT BUỘC TRẢ VỀ JSON ARRAY VÀ KHÔNG GIẢI THÍCH GÌ THÊM. BẮT BUỘC giữ đúng "comment_id"

CẤU TRÚC OBJECT JSON YÊU CẦU:
{
    "comment_id": "id_của_comment_đầu_vào",
    "labels": "nhãn_dự_đoán_ban_đầu_của_hệ_thống",
    "truth_labels": "nhãn_chính_xác_cuối_cùng_sau_khi_bạn_kiểm_tra_và_sửa",
    "reason": "Giải thích tiếng Việt ngắn gọn: Tại sao bạn đồng ý giữ nguyên nhãn cũ, hoặc tại sao bạn quyết định bẻ lái đổi sang nhãn mới dựa trên ngữ cảnh."
}
"""


def analyze_emotions(texts):
    text_flats = "\n".join(" - " + text for text in texts)
    retry_count = 0
    
    while True:
        try:
            response = client.models.generate_content(
                model="gemma-4-31b-it", 
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    response_mime_type="application/json",
                    temperature=0.1, 
                ),
                contents=[
                    f"==================\nPhân tích cảm xúc các câu sau và trả về JSON array. Nhớ giữ nguyên ID:\n{text_flats}"
                ],
            )

            batch_json = json.loads(response.text)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Dịch thành công {len(batch_json)} dòng.")
            
            # Sleep 5 seconds to avoid rate limit
            time.sleep(5) 
            return batch_json

        except Exception as e:
            retry_count += 1
            wait_time = 5 * retry_count # Wait 5, 10, 15, ... seconds for each retry
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ERR (Try again after {wait_time}s): {e}")
            time.sleep(wait_time)

def main():
    lf = pl.scan_parquet(INPUT_FILE).select(["comment_id", "text", "labels"])
    total = lf.select(pl.len()).collect().item()

    start_time = time.time()
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

        # ---------------------------------------------------------
        # CALC ELAPSED TIME & ETA AFTER EVERY BATCH
        # ---------------------------------------------------------
        processed_items = min(i + BATCH_SIZE, total)
        percent_done = (processed_items / total) * 100

        elapsed_seconds = time.time() - start_time

        if processed_items > 0:
            speed = processed_items / elapsed_seconds
            
            remaining_items = total - processed_items
            eta_seconds = remaining_items / speed
            
            elapsed_str = str(timedelta(seconds=int(elapsed_seconds)))
            eta_str = str(timedelta(seconds=int(eta_seconds)))
            
            current_time = datetime.now().strftime('%H:%M:%S')
            print(f"[{current_time}] Processed: {processed_items}/{total} ({percent_done:.2f}%) | Run: {elapsed_str} | Est (ETA): {eta_str}")



if __name__ == "__main__":
    main()
