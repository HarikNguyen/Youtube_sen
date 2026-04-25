import os
import re
import time
import shutil
import random
import requests
import threading
import datetime
import pandas as pd
import yt_dlp
from concurrent.futures import ThreadPoolExecutor, as_completed
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import JSONFormatter
from youtube_transcript_api.proxies import GenericProxyConfig

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG
# ==========================================
ROOT_DIR = "raw_data"
COOKIE_FILES = ["cookies_1.txt", "cookies_2.txt", "cookies_3.txt", "cookies_4.txt"]
SHUFFLE_INTERVAL = 300  # 5 phút xáo trộn chéo Proxy và Cookie 1 lần
MAX_THREADS_PER_PROXY = 10  # Mỗi proxy gánh 10 luồng (Tổng 40 luồng)

PROXIES_CONFIG = [
    {
        "addr": "180.93.2.169:3130",
        "user": "berlinartman7",
        "pass": "mtk2mzgynzcx",
        "rotate_url": "https://app.homeproxy.vn/api/v3/users/rotatev2?token=MTc3NjA2MTY1OTU3MztiZXJsaW5hcnRtYW43O2FwaS1wcm94eS0yLmhvbWVwcm94eS52bg==_a3a1babf068bdddaa4b34ee9481d9bdd3dc36464",
    },
    {
        "addr": "180.93.2.169:3129",
        "user": "warrenherman790",
        "pass": "mtk2mzgynzcx",
        "rotate_url": "https://app.homeproxy.vn/api/v3/users/rotatev2?token=MTc3NjA2MTY1OTU3Mzt3YXJyZW5oZXJtYW43OTA7YXBpLXByb3h5LTIuaG9tZXByb3h5LnZu_2d6d2914ace1b37833a786d2bc773ecea4975530",
    },
    {
        "addr": "180.93.2.169:3130",
        "user": "bodhibarmo6395",
        "pass": "mtk2mzgynzcx",
        "rotate_url": "https://app.homeproxy.vn/api/v3/users/rotatev2?token=MTc3NjA2MTY1OTU3Mztib2RoaWJhcm1vNjM5NTthcGktcHJveHktMi5ob21lcHJveHkudm4=_4b874cf5fbfd07e2cb89d20fc9214f1feb85e57d",
    },
    {
        "addr": "103.216.74.218:4454",
        "user": "reagan61616",
        "pass": "ntu4odqwmtmx",
        "rotate_url": "https://app.homeproxy.vn/api/v3/users/rotatev2?token=MTc3NjA5NzYzNTc2ODtyZWFnYW42MTYxNjthcGktcHJveHkuaG9tZXByb3h5LnZu_488353334acce0edc9483c77c6e3c4e796e591b3",
    },
]

assert len(PROXIES_CONFIG) == len(
    COOKIE_FILES
), "CẢNH BÁO: Số lượng Proxy phải bằng đúng số lượng file Cookie!"


# ==========================================
# 2. CÁC LỚP QUẢN LÝ VÀ BỘ LỌC
# ==========================================
class MuteLogger:
    """Bộ lọc này giúp tắt hoàn toàn các dòng chữ đỏ báo lỗi rác của yt-dlp"""

    def debug(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass


class SessionManager:
    """Quản lý việc ghép cặp Proxy - Cookie và xoay IP"""

    def __init__(self, proxies, cookies):
        self.proxies = proxies
        self.cookies = list(cookies)

        self.ip_locks = {c["addr"]: threading.Lock() for c in proxies}
        self.last_rotate_ip = {c["addr"]: 0 for c in proxies}

        self.shuffle_lock = threading.Lock()
        self.last_shuffled = time.time()

    def get_session(self, index):
        with self.shuffle_lock:
            if time.time() - self.last_shuffled > SHUFFLE_INTERVAL:
                print(
                    "\n[SYSTEM] Thực hiện xáo trộn (Shuffle) ghép cặp Proxy - Cookie để né nhận diện..."
                )
                random.shuffle(self.cookies)
                self.last_shuffled = time.time()
                for i in range(len(self.proxies)):
                    print(
                        f"  -> Proxy {i+1} ({self.proxies[i]['addr']}) được gắn với {self.cookies[i]}"
                    )
                print()

            idx = index % len(self.proxies)
            proxy_conf = self.proxies[idx]
            cookie_file = self.cookies[idx]

        proxy_url = (
            f"http://{proxy_conf['user']}:{proxy_conf['pass']}@{proxy_conf['addr']}"
        )
        return proxy_url, cookie_file, proxy_conf

    def rotate_ip(self, proxy_conf):
        addr = proxy_conf["addr"]
        with self.ip_locks[addr]:
            if time.time() - self.last_rotate_ip[addr] > 120:
                try:
                    print(f"\n[PROXY] Đang yêu cầu IP mới cho cổng {addr}...")
                    requests.get(proxy_conf["rotate_url"], timeout=10)
                    self.last_rotate_ip[addr] = time.time()
                    time.sleep(15)  # Đợi 15s để IP mới có hiệu lực
                    return True
                except Exception as e:
                    print(f"[PROXY ERROR] Xoay IP thất bại: {e}")
        return False


session_manager = SessionManager(PROXIES_CONFIG, COOKIE_FILES)


# ==========================================
# 3. HÀM PHỤ TRỢ (HELPER)
# ==========================================
def extract_video_id(url):
    match = re.search(r"(?:v=|\/)([0-9A-Za-z_-]{11}).*", str(url))
    return match.group(1) if match else None


def get_transcript_safe(video_id, proxy_config, session_manager, max_retries=2):
    """
    CÁCH 2: Tự động retry và xoay IP ngay bên trong hàm lấy Transcript.
    Không làm ảnh hưởng đến tiến độ cào Comment của yt-dlp.
    """
    for attempt in range(max_retries + 1):
        # Lấy URL proxy hiện tại (cập nhật liên tục lỡ có thay đổi sau khi rotate)
        proxy_url = f"http://{proxy_config['user']}:{proxy_config['pass']}@{proxy_config['addr']}"

        try:
            proxies_dict = GenericProxyConfig(http_url=proxy_url, https_url=proxy_url)
            ytt_api = YouTubeTranscriptApi(proxy_config=proxies_dict)
            transcript_list = ytt_api.list(video_id)

            try:
                ts = transcript_list.find_manually_created_transcript(["vi", "en"])
            except:
                ts = transcript_list.find_generated_transcript(["vi", "en"])

            ts_list = ts.fetch()
            return JSONFormatter().format_transcript(ts_list)

        except Exception as e:
            err_msg = str(e).lower()

            # Nếu lỗi mạng/bị chặn IP và vẫn còn lượt thử
            if attempt < max_retries and any(
                x in err_msg for x in ["403", "proxy", "connection", "forbidden"]
            ):
                print(
                    f"[TRANSCRIPT RETRY] Cổng {proxy_config['addr']} bị chặn khi lấy sub. Đang xoay IP (Lần {attempt + 1}/{max_retries})..."
                )
                session_manager.rotate_ip(proxy_config)
                continue  # Nhảy sang lượt thử tiếp theo trong vòng lặp for

            # Trả về chuỗi lỗi thực sự (do hết lượt thử hoặc do video không có sub thật)
            return f"Transcript unavailable: {str(e)}"


# ==========================================
# 4. HÀM XỬ LÝ CHÍNH (CRAWL CORE)
# ==========================================
def process_single_video(video_task, thread_index, retry_count=1):
    url = video_task["url"]
    video_id = video_task["video_id"]
    category = "".join(
        [
            c
            for c in video_task.get("category", "Uncategorized")
            if c.isalnum() or c == " "
        ]
    ).strip()
    channel = "".join(
        [c for c in video_task.get("channel", "Unknown") if c.isalnum() or c == " "]
    ).strip()

    final_dir = os.path.join(ROOT_DIR, category, channel, video_id)
    temp_dir = os.path.join(ROOT_DIR, category, channel, f"x____{video_id}")

    if os.path.exists(final_dir):
        return {"status": "skipped", "video_id": video_id}

    try:
        os.makedirs(temp_dir, exist_ok=True)
    except FileExistsError:
        # This happens if another thread created it between the check and the makedirs call
        pass

    proxy_url, current_cookie, proxy_config = session_manager.get_session(thread_index)

    # CẤU HÌNH YT-DLP GỐC
    ydl_opts_base = {
        "skip_download": True,
        "getcomments": True,
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": False,  # Chuyển thành False để bắt chính xác lỗi 403
        "ignore_no_formats_error": True,  # Bỏ qua lỗi format livestream đỏ màn hình
        "logger": MuteLogger(),  # Tắt tiếng các lỗi rác còn lại
        "proxy": proxy_url,
        "cookiefile": current_cookie,
        "extractor_args": {
            "youtube": {
                "player_client": ["mweb", "web"],
                "max_comments": ["2000", "all", "all"],  # Lấy max 2k gốc mỗi lần quét
            }
        },
        "http_headers": {
            "User-Agent": "Mozilla/5.0 (Linux; Android 14; Mobile) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        },
    }

    try:
        dict_comments = {}
        metadata = {}

        # CHIẾN THUẬT: QUÉT 2 VÒNG (TOP và NEW)
        for sort_type in ["top", "new"]:
            ydl_opts_base["extractor_args"]["youtube"]["comment_sort"] = [sort_type]

            with yt_dlp.YoutubeDL(ydl_opts_base) as ydl:
                info = ydl.extract_info(url, download=False)

                if not info:
                    raise Exception(f"Bị chặn khi lấy dữ liệu vòng '{sort_type}'")

                # Lưu metadata ở vòng đầu tiên
                if sort_type == "top":
                    metadata = {
                        "video_id": video_id,
                        "title": info.get("title"),
                        "channel": info.get("uploader"),
                        "view_count": info.get("view_count"),
                        "like_count": info.get("like_count"),
                        "upload_date": info.get("upload_date"),
                    }

                # GOM COMMENT VÀ LỌC TRÙNG THEO ID
                if "comments" in info and info["comments"]:
                    for c in info["comments"]:
                        c_id = c.get("id")
                        if c_id not in dict_comments:

                            # Xử lý Timestamp chuẩn
                            raw_timestamp = c.get("timestamp")
                            formatted_date = (
                                datetime.datetime.fromtimestamp(raw_timestamp).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                                if raw_timestamp
                                else ""
                            )
                            parent_id = c.get("parent", "root")

                            dict_comments[c_id] = {
                                "comment_id": c_id,
                                "parent_id": parent_id,
                                "is_reply": parent_id != "root",
                                "author": c.get("author"),
                                "comment": c.get("text", ""),
                                "like_count": c.get("like_count", 0),
                                "time_text": c.get("time_text", ""),
                                "created_at": formatted_date,
                            }

        # Ghi file Metadata
        pd.DataFrame([metadata]).to_csv(
            os.path.join(temp_dir, "metadata.csv"), index=False, encoding="utf-8-sig"
        )

        # Ghi file Comments (Đã lọc trùng Unique)
        unique_comments_list = list(dict_comments.values())
        if unique_comments_list:
            pd.DataFrame(unique_comments_list).to_csv(
                os.path.join(temp_dir, "comment.csv"), index=False, encoding="utf-8-sig"
            )

        # Ghi Transcript (Sử dụng hàm mới đã cập nhật truyền thêm config và session)
        ts_data = get_transcript_safe(video_id, proxy_config, session_manager)
        with open(
            os.path.join(temp_dir, "transcript.json"), "w", encoding="utf-8"
        ) as f:
            f.write(ts_data)

        os.rename(temp_dir, final_dir)
        return {"status": "success", "video_id": video_id}

    except Exception as e:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        err_msg = str(e).lower()

        # Xử lý lỗi 403 / Proxy chết của yt-dlp -> Xoay vòng IP và chạy lại nguyên hàm process_single_video
        if any(x in err_msg for x in ["403", "proxy", "connection", "forbidden"]):
            if retry_count > 0:
                print(
                    f"[RETRY YT-DLP] Lỗi mạng/403. Đang xoay IP cho {proxy_config['addr']} và cào lại từ đầu..."
                )
                session_manager.rotate_ip(proxy_config)
                return process_single_video(video_task, thread_index, retry_count=0)

        # Xử lý lỗi Tài khoản chết -> Báo động đỏ
        if "sign in" in err_msg or "verify" in err_msg:
            print(
                f"\n[CRITICAL] TÀI KHOẢN TRONG {current_cookie} đi kèm {proxy_config['addr']} ĐÃ CHẾT/YÊU CẦU ĐĂNG NHẬP!"
            )

        return {"status": "error", "error": str(e), "video_id": video_id}


# ==========================================
# 5. BỘ ĐIỀU PHỐI (ORCHESTRATOR)
# ==========================================
def scrape_youtube_fast(video_tasks_gen):
    print("[SYSTEM] Khởi động mồi Plugin yt-dlp để tránh xung đột...")
    with yt_dlp.YoutubeDL({"quiet": True}) as _dummy:
        pass

    max_threads = len(PROXIES_CONFIG) * MAX_THREADS_PER_PROXY
    print(
        f"[START] Đang chạy với {max_threads} threads (10 threads/Proxy) và {len(PROXIES_CONFIG)} Proxies..."
    )

    progress = {"success": 0, "errors": 0, "skipped": 0}
    progress_lock = threading.Lock()
    is_running = True

    # Luồng ghi Log chạy ngầm
    def logger_worker():
        while is_running:
            time.sleep(30)
            if not is_running:
                break

            with progress_lock:
                print(f"\n--- [LOG STATUS @ {time.strftime('%H:%M:%S')}] ---")
                print(
                    f"  > Progress     : {progress['success']} Suscess | {progress['skipped']} Skip | {progress['errors']} Error"
                )
                print(f"  > Total Processed: {sum(progress.values())}")
                print(f"----------------------------------------\n")

    threading.Thread(target=logger_worker, daemon=True).start()

    # Xử lý đa luồng (Multi-threading)
    try:
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_video = {}

            while True:
                try:
                    while len(future_to_video) < max_threads:
                        task = next(video_tasks_gen)
                        thread_index = len(future_to_video) % len(PROXIES_CONFIG)
                        fut = executor.submit(process_single_video, task, thread_index)
                        future_to_video[fut] = task["video_id"]

                    for future in as_completed(future_to_video):
                        res = future.result()
                        future_to_video.pop(future)

                        with progress_lock:
                            status = res.get("status")
                            if status in progress:
                                progress[status] += 1
                        break

                except StopIteration:
                    break

            for future in as_completed(future_to_video):
                res = future.result()
                with progress_lock:
                    status = res.get("status")
                    if status in progress:
                        progress[status] += 1

    finally:
        is_running = False
        print(
            f"\n[FINISH] Hoàn thành. Thành công: {progress['success']} | Lỗi: {progress['errors']}"
        )


def task_generator(csv_path):
    for chunk in pd.read_csv(csv_path, chunksize=1000):
        for _, row in chunk.iterrows():
            url = row.get("video_url")
            vid = extract_video_id(url)
            if vid:
                yield {
                    "url": url,
                    "video_id": vid,
                    "category": row.get("category", "Uncategorized"),
                    "channel": row.get("title", "UnknownChannel"),
                }


# ==========================================
# 6. KHỞI CHẠY (MAIN)
# ==========================================
if __name__ == "__main__":
    CSV_INPUT = "video_stats_final_v2.csv"  # Đổi tên file CSV của bạn ở đây
    if not os.path.exists(ROOT_DIR):
        os.makedirs(ROOT_DIR)

    gen = task_generator(CSV_INPUT)
    scrape_youtube_fast(gen)
