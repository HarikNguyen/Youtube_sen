import pandas as pd
import scrapetube
import csv
import os
import requests
import time
import threading
import random
from concurrent.futures import ThreadPoolExecutor

# --- FILE CONFIGURATION ---
INPUT_FILE = 'expanded.csv'
OUTPUT_FILE = 'res.csv'

# --- PROXY CONFIGURATION (TRIPLE GATEWAY) ---
PROXIES = []

# Example:
# PROXIES = [
    # {
        # "addr": "180.93.2.169:3129",
        # "user": "garrickbrown685",
        # "pass": "mzewnjawmjg4",
        # "rotate_url": "https://app.homeproxy.vn/api/v3/users/rotatev2?token=MTc3NTcxMTE1MDk3NztnYXJyaWNrYnJvd242ODU7YXBpLXByb3h5LTIuaG9tZXByb3h5LnZu_0e08b75ad6d251273c0e0055096515a24154de22"
    # },
    # {
        # "addr": "180.93.2.169:3129",
        # "user": "chazreichel7240",
        # "pass": "mzewnjawmjg4",
        # "rotate_url": "https://app.homeproxy.vn/api/v3/users/rotatev2?token=MTc3NTcxMTE1MDk3NztjaGF6cmVpY2hlbDcyNDA7YXBpLXByb3h5LTIuaG9tZXByb3h5LnZu_9de577b52fb3fdfd7ef347df1bf7401966375f36"
    # },
    # {
        # "addr": "103.216.74.218:4454",
        # "user": "reagan61616",
        # "pass": "ntu4odqwmtmx",
        # "rotate_url": "https://app.homeproxy.vn/api/v3/users/rotatev2?token=MTc3NjA5NzYzNTc2ODtyZWFnYW42MTYxNjthcGktcHJveHkuaG9tZXByb3h5LnZu_488353334acce0edc9483c77c6e3c4e796e591b3"
    # }
# ]

# --- PERFORMANCE TUNING (TURBO MODE) ---
# High concurrency for 200Mbps network and 3 proxies
MAX_WORKERS = 45  
BATCH_SIZE = 90   

# Thread-safety locks
csv_lock = threading.Lock()
rotation_lock = threading.Lock()
last_rotation_time = 0

def set_proxy(proxy_config):
    """Sets the proxy for the current batch execution environment"""
    auth = f"{proxy_config['user']}:{proxy_config['pass']}@{proxy_config['addr']}"
    os.environ['HTTP_PROXY'] = f"http://{auth}"
    os.environ['HTTPS_PROXY'] = f"http://{auth}"
    print(f"\n[#] ACTIVE PROXY: {proxy_config['user']}")

def rotate_all_proxies():
    """Triggers IP rotation for all three proxy accounts simultaneously"""
    print("\n[!] Triggering Triple IP Rotation...")
    for p in PROXIES:
        try:
            # Request new IP from provider API
            requests.get(p['rotate_url'], timeout=10)
        except Exception as e:
            print(f" [X] Rotation failed for {p['user']}: {e}")
    
    # Wait for the proxy network to finalize the IP switch
    time.sleep(12) 

def safe_rotate():
    """Thread-safe rotation call with cooldown to prevent API spamming"""
    global last_rotation_time
    with rotation_lock:
        current_time = time.time()
        if current_time - last_rotation_time > 45:
            rotate_all_proxies()
            last_rotation_time = current_time
        else:
            print("[!] Rotation skipped: Cooldown still active.")

def process_channel(channel_id):
    """Core function to extract all content types from a single YouTube channel"""
    base_url = "https://www.youtube.com/watch?v="
    all_video_links = []
    
    print(f"[*] Processing: {channel_id}")

    # Scrape all three content types provided by YouTube
    for content_type in ['videos', 'shorts', 'streams']:
        success = False
        retries = 2
        
        while not success and retries > 0:
            try:
                # ScrapeTube uses the proxy set in os.environ
                videos = scrapetube.get_channel(channel_id, content_type=content_type)
                
                for v in videos:
                    v_id = v['videoId']
                    all_video_links.append([channel_id, content_type, f"{base_url}{v_id}"])
                
                success = True
            except Exception as e:
                retries -= 1
                error_str = str(e)
                # If network or SSL issues occur, attempt a rotation and retry
                if any(err in error_str for err in ["SSL", "EOF", "429", "Connection"]):
                    print(f" [!] SSL/Rate Limit on {channel_id} ({content_type}). Rotating...")
                    safe_rotate()
                else:
                    break

    # Save results to CSV immediately to ensure data persistence
    with csv_lock:
        with open(OUTPUT_FILE, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if all_video_links:
                writer.writerows(all_video_links)
            else:
                # Mark channel as checked even if no content is found
                writer.writerow([channel_id, 'none', 'no_content'])
    
    print(f"[V] COMPLETED: {channel_id} | Total links: {len(all_video_links)}")

def main():
    # 1. INITIALIZATION & RESUME LOGIC
    if not os.path.exists(INPUT_FILE):
        print(f"FATAL ERROR: {INPUT_FILE} is missing!")
        return

    # Load all target IDs
    df_in = pd.read_csv(INPUT_FILE)
    all_ids = df_in['channel_id'].unique().tolist()

    # Filter out IDs that have already been processed
    if os.path.exists(OUTPUT_FILE):
        try:
            df_out = pd.read_csv(OUTPUT_FILE)
            done_ids = set(df_out['channel_id'].unique())
            to_scan = [cid for cid in all_ids if cid not in done_ids]
            print(f"RESUME: {len(done_ids)} finished | {len(to_scan)} remaining.")
        except Exception:
            print("Status: Output file corrupted or empty. Starting fresh.")
            to_scan = all_ids
    else:
        to_scan = all_ids
        # Create CSV header if file doesn't exist
        with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(['channel_id', 'video_type', 'video_url'])

    if not to_scan:
        print("Success: All channels have been processed.")
        return

    # 2. BATCH EXECUTION ENGINE
    rotate_all_proxies() # Initial rotation for fresh start

    for i in range(0, len(to_scan), BATCH_SIZE):
        current_batch = to_scan[i : i + BATCH_SIZE]
        
        # Balance the load by cycling which proxy the batch starts with
        current_proxy = PROXIES[(i // BATCH_SIZE) % len(PROXIES)]
        set_proxy(current_proxy)
        
        print(f"\n>>> EXECUTING BATCH {i//BATCH_SIZE + 1} ({len(current_batch)} channels)")
        print(f">>> Tasks remaining: {len(to_scan) - i}")

        # Parallelize the scraping across 45 workers
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(process_channel, current_batch)
        
        # Rotate IPs after every batch to keep the scraper "anonymous"
        if (i + BATCH_SIZE) < len(to_scan):
            safe_rotate()

    print("\n[COMPLETE] All links have been harvested. Check res.csv.")

if __name__ == "__main__":
    main()
