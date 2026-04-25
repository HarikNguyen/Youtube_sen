import os
import json
import glob
import time
import requests
import pandas as pd
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound,
    CouldNotRetrieveTranscript,
)
from youtube_transcript_api.formatters import JSONFormatter
from youtube_transcript_api.proxies import GenericProxyConfig


def flatten_transcript(data):
    if isinstance(data, list):
        # Concat all 'text' elements
        return " ".join(
            [item.get("text", "") for item in data if isinstance(item, dict)]
        ).strip()
    elif isinstance(data, dict):
        # Return 'text' if it is a dict, otherwise convert to string
        return data.get("text", "")
    return str(data)


def process_transcript_files(root_path, max_retries=10, delay=1):
    video_df = pd.read_parquet("raw_videos.parquet")

    for row in video_df.itertuples():
        video_id = row.video_id
        file_path = os.path.join(
            root_path, row.category, row.channel, video_id, "transcript.json"
        )

        content = ""
        success = False

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                content = flatten_transcript(data)
            success = True
        except (json.JSONDecodeError, IOError, Exception) as e:
            print(f"Lỗi khi đọc {file_path}: {e}")
            content = try_get_transcript_via_api(video_id, max_retries, delay)
        video_df.at[row.Index, "transcript"] = content
        print("---" * 10)

    return video_df


def try_get_transcript_via_api(video_id, max_retries=3, delay=1):
    proxy_url = f"http://dantelabadie232:nzcxnzgzmdk5@180.93.2.169:3129"
    proxies_dict = GenericProxyConfig(http_url=proxy_url, https_url=proxy_url)
    ytt_api = YouTubeTranscriptApi(proxy_config=proxies_dict)

    for attempt in range(max_retries):

        try:
            transcript_list = ytt_api.list(video_id)
            try:
                ts = transcript_list.find_manually_created_transcript(["vi", "en"])
            except:
                ts = transcript_list.find_generated_transcript(["vi", "en"])
            print("---")

            ts_list = ts.fetch()
            ts_list = JSONFormatter().format_transcript(ts_list)
            return flatten_transcript(ts_list)

        except (TranscriptsDisabled, NoTranscriptFound) as e:
            print(f"Transcript for video {video_id} disabled or not found.")
            return ""

        except (CouldNotRetrieveTranscript, Exception) as e:
            error_msg = str(e)
            if "blocking" in error_msg or "IP" in error_msg:
                print(f"The {attempt + 1} attempt failed: YouTube blocking IP/Proxy.")
                if attempt < max_retries - 1:
                    print(f"Retrying in {delay} seconds...")
                    rotate_proxy()
                    time.sleep(delay)
            else:
                print(f"The {attempt + 1} attempt failed.\nAs err: {error_msg}")
                return ""

        except Exception as e:
            print(f"The {attempt + 1} attempt failed.\nAs err: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {delay} seconds...")
                rotate_proxy()
                time.sleep(delay)
            else:
                return ""


def rotate_proxy():
    rotate_url = "https://app.homeproxy.vn/api/v3/users/rotatev2?token=MTc3NjE3NTgwNjI2NjtkYW50ZWxhYmFkaWUyMzI7YXBpLXByb3h5LTIuaG9tZXByb3h5LnZu_bc961801dc257f814b983cc8430653707f18427d"

    while True:
        try:
            response = requests.get(rotate_url)
            if response.status_code == 200:
                print("Proxy rotated successfully.")
                break
            else:
                print(f"Failed to rotate proxy. Status code: {response.status_code}")
        except Exception as e:
            print(f"Error rotating proxy: {e}")
        print("Retrying in 5 seconds...")
        time.sleep(5)


# --- SỬ DỤNG ---
# Giả sử thư mục gốc của bạn là 'raw_data'
# root_dir = 'raw_data'
# all_transcripts = process_transcript_files(root_dir)

# # Ví dụ: In ra 1 kết quả đầu tiên để kiểm tra
# if all_transcripts:
# first_key = list(all_transcripts.keys())[0]
# print(f"\nVideo ID: {first_key}")
# print(f"Nội dung (flatten): {all_transcripts[first_key][:200]}...") # In 200 ký tự đầu

if __name__ == "__main__":
    root_dir = "raw_data"
    v_df = process_transcript_files(root_dir)
    v_df.to_parquet("processed_videos.parquet", index=False)
