import pandas as pd
import polars as pl
import json
import time
import os
import threading
from datetime import datetime
from google import genai
from google.genai import types

INPUT_FILE = "<file_to_in>.parquet"
LABEL = "<label: example: embarrassment>"
OUTPUT_FILE = f"generated_1500_{LABEL}.csv"
API_KEY = "<google_ai_studio_api_here>"
TOTAL_NEEDED = 1500
BATCH_SIZE = 50 
MAX_WORKERS = 3 

client = genai.Client(api_key=API_KEY)
file_lock = threading.Lock()
current_count = 0
count_lock = threading.Lock()

SYSTEM_PROMPT = """
Bạn là một chuyên gia ngôn ngữ học. Tôi sẽ cung cấp ví dụ về comment YouTube.
Nhiệm vụ: Sáng tạo thêm 50 comment mới HOÀN TOÀN KHÁC BIỆT mang nhãn 'embarrassment' (xấu hổ, ngượng ngùng, khó xử, "sượng trân").

Mỗi dòng dữ kiện phải tuân theo dạng như sau:
    - [TITLE] <video title> [CHANNEL] <channel name> [CATEGORY] <category name> [COMMENT] <comment> (Nếu comment này là parent comment)
    - [TITLE] <video title> [CHANNEL] <channel name> [CATEGORY] <category name> [COMMENT] <comment> [REPLY] <reply> (Nếu comment này reply cho parent comment)

QUY TẮC:
1. Văn phong: Giống người dùng YouTube Việt Nam. Sử dụng các cụm từ thể hiện sự ngại ngùng như: "xem mà ngại giùm", "ngượng chín mặt", "muốn độn thổ", "sượng trân", "cringe quá", "không dám xem hết", "tới khúc này tắt máy luôn vì ngại"...
2. Ngữ cảnh: Có thể là sự xấu hổ của chính người viết hoặc cảm giác ngượng giùm cho YouTuber/nhân vật trong video do họ có hành động vụng về, lỡ lời hoặc tự tin thái quá.
3. Định dạng: TRẢ VỀ JSON ARRAY.
4. Mỗi object: {"text": "nội dung theo cấu trúc [TITLE]...", "labels": "embarrassment", "reason": "giải thích ngắn gọn tại sao câu này thể hiện sự xấu hổ/ngượng ngùng"}
5. KHÔNG giải thích gì ngoài JSON.
"""

def prepare_examples():
    """Get examples from parquet file."""
    df = pl.read_parquet(INPUT_FILE)
    label_examples = df.filter(pl.col("labels") == LABEL).head(2)
    other_labels = [l for l in df["labels"].unique().to_list() if l != LABEL]
    others = []
    for label in other_labels:
        others.append(df.filter(pl.col("labels") == label).head(2))
    other_examples = pl.concat(others).head(54)
    combined = pl.concat([label_examples, other_examples])
    
    example_text = ""
    for row in combined.iter_rows(named=True):
        example_text += f"- [{row['labels']}]: {row['text']}\n"
    return example_text

def generation_worker(examples):
    global current_count
    
    while True:
        with count_lock:
            if current_count >= TOTAL_NEEDED:
                break
        
        try:
            response = client.models.generate_content(
                model="gemini-3.1-flash-lite-preview", 
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    response_mime_type="application/json",
                    temperature=0.9,
                ),
                contents=[f"Dựa trên các ví dụ này:\n{examples}\n\nHãy tạo 50 câu mới mang nhãn '{LABEL}'."],
            )

            batch_data = json.loads(response.text)
            df_batch = pd.DataFrame(batch_data)

            with file_lock:
                header = not os.path.exists(OUTPUT_FILE)
                df_batch.to_csv(OUTPUT_FILE, mode='a', index=False, header=header, encoding="utf-8-sig")
                
            with count_lock:
                current_count += len(df_batch)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Progress: {current_count}/{TOTAL_NEEDED}")

        except Exception as e:
            print(f"ERR: {str(e)[:100]}. Retrying...")
            time.sleep(5)

def main():
    print(f"Generating {TOTAL_NEEDED} with '{LABEL}'...")
    examples = prepare_examples()
    
    threads = []
    for i in range(MAX_WORKERS):
        t = threading.Thread(target=generation_worker, args=(examples,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print(f"--- COMPLETED ---")
    print(f"- Output file: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
