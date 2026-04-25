import pandas as pd
import re
import os
import time
from googleapiclient.errors import HttpError


# Custom exception to identify when the API quota is exhausted
class QuotaExceededError(Exception):
    pass


def extract_id(url):
    """Extracts Video ID from a YouTube URL or returns the string if it is an ID."""
    if pd.isna(url):
        return None
    regex = r"(?:v=|\/)([0-9A-Za-z_-]{11}).*"
    match = re.search(regex, str(url))
    return match.group(1) if match else str(url).strip()


def fetch_comment_counts_batch(youtube_client, video_ids):
    """Fetches comment counts for up to 50 video IDs with a retry mechanism."""
    for attempt in range(3):
        try:
            request = youtube_client.videos().list(
                part="statistics",
                id=",".join(video_ids),
                fields="items(id,statistics/commentCount)",
            )
            response = request.execute()

            # Map video IDs to their respective comment counts
            stats_map = {
                item["id"]: item["statistics"].get("commentCount", "0")
                for item in response.get("items", [])
            }
            return stats_map

        except HttpError as e:
            if e.resp.status == 403:
                error_content = e.content.decode("utf-8").lower()
                if (
                    "quotaexceeded" in error_content
                    or "ratelimitexceeded" in error_content
                ):
                    raise QuotaExceededError("API Key quota exceeded.")

            # Exponential backoff for other HttpErrors
            if attempt < 2:
                time.sleep(2**attempt)
                continue
            print(f"API HttpError for batch: {e}")
            return {}

        except Exception as e:
            if attempt < 2:
                time.sleep(2**attempt)
                continue
            print(f"API Error for batch: {e}")
            return {}


def track_comment_counts(
    key_manager, input_path, ignore_path, output_path, chunk_size=10000
):
    """Main function to process video data with Resume, time-based reporting, and Auto Key Rotation."""

    print(f"--- Loading exclusion list from {ignore_path} ---")
    ignore_ids = set()
    if os.path.exists(ignore_path):
        try:
            df_ignore = pd.read_csv(ignore_path)
            ignore_ids = set(df_ignore["channel_id"].astype(str).unique())
        except Exception as e:
            print(f"Warning: Could not read ignore file: {e}")

    processed_vids = set()
    if os.path.exists(output_path):
        print(f"--- Scanning existing output for resume ---")
        try:
            df_existing = pd.read_csv(output_path, usecols=["video_id"])
            processed_vids = set(df_existing["video_id"].dropna().astype(str).unique())
            print(f"--- Found {len(processed_vids)} existing records in output ---")
        except Exception:
            print("--- Output file format issue. Starting fresh. ---")

    start_time = time.time()
    last_report_time = start_time
    report_interval = 30

    print(f"--- Starting processing {input_path} ---")
    reader = pd.read_csv(input_path, chunksize=chunk_size)
    is_first_chunk = not os.path.exists(output_path)
    total_new_processed = 0

    youtube_client = key_manager.get_client()

    for chunk in reader:
        chunk = chunk[~chunk["channel_id"].astype(str).isin(ignore_ids)].copy()
        chunk["video_id"] = chunk["video_url"].apply(extract_id)
        new_items_df = chunk[~chunk["video_id"].isin(processed_vids)].copy()

        if new_items_df.empty:
            continue

        vids_to_query = new_items_df["video_id"].dropna().unique().tolist()
        batches = [vids_to_query[i : i + 50] for i in range(0, len(vids_to_query), 50)]

        all_stats = {}
        print(f"--- Fetching comment counts for {len(batches)} batches ---")

        for batch in batches:
            while True:
                try:
                    batch_results = fetch_comment_counts_batch(youtube_client, batch)
                    all_stats.update(batch_results)
                    break
                except QuotaExceededError:
                    print("\n[!] Warning: Quota exhausted. Rotating API Key...")
                    youtube_client = key_manager.rotate_key()

        new_items_df["comment_count"] = new_items_df["video_id"].map(
            lambda x: all_stats.get(x, "N/A")
        )

        mode = "w" if is_first_chunk else "a"
        header = is_first_chunk
        new_items_df.to_csv(
            output_path, index=False, mode=mode, header=header, encoding="utf-8-sig"
        )

        is_first_chunk = False
        total_new_processed += len(new_items_df)

        current_time = time.time()
        if current_time - last_report_time >= report_interval:
            elapsed_total = current_time - start_time
            print(
                f"--- Time-based Update: {total_new_processed} new videos processed. (Elapsed: {int(elapsed_total)}s) ---"
            )
            last_report_time = current_time

    print(
        f"--- Task Finished. Total new processed: {total_new_processed}. Results: {output_path} ---"
    )
