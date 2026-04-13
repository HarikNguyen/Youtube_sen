import os
import argparse
import json
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build
from search_cate import create_seed
from expand_seed import simulate_recommendation_engine
from crawl import crawl
from check_is_vn_channel import is_vn_channels
from tracker_yt_info import track_comment_counts

load_dotenv()
API_KEY = os.getenv("API_KEY")
HTTP_PROXY = os.getenv("HTTP_PROXY")
HTTPS_PROXY = os.getenv("HTTPS_PROXY")

# Set up proxy environment variables if they exist
if HTTP_PROXY:
    os.environ['http_proxy'] = HTTP_PROXY
    os.environ['HTTP_PROXY'] = HTTP_PROXY
if HTTPS_PROXY:
    os.environ['https_proxy'] = HTTPS_PROXY
    os.environ['HTTPS_PROXY'] = HTTPS_PROXY

class APIKeyManager:
    def __init__(self, key_file_path):
        self.keys = []
        if os.path.exists(key_file_path):
            with open(key_file_path, 'r') as f:
                # Read line by line, skip empty lines
                self.keys = [line.strip() for line in f if line.strip()]
        
        # If .api file is empty or missing, fallback to .env
        if not self.keys:
            env_key = os.getenv("API_KEY")
            if env_key:
                self.keys.append(env_key)
            else:
                raise ValueError("No API Key found in .api or .env file!")
                
        self.current_index = 0
        print(f"Loaded {len(self.keys)} API Keys.")

    def get_client(self):
        return build("youtube", "v3", developerKey=self.keys[self.current_index])

    def rotate_key(self):
        self.current_index += 1
        if self.current_index >= len(self.keys):
            raise Exception("ALL API Keys have reached their Quota. Process stopped!")
        
        print(f"[*] Switched to API Key #{self.current_index + 1}/{len(self.keys)}")
        return self.get_client()

def process_parser(args):
    key_manager = APIKeyManager(".api")
    youtube_client = key_manager.get_client()

    if args.create_seed:
        print("=="*60)
        print("Create Seed")
        print("=="*60)
        seed = create_seed(youtube_client)
        with open("seed.json", "w") as f:
            json.dump(seed, f, indent=4)

    if args.expand_seed:
        print("=="*60)
        print("Expand Seed")
        print("=="*60)
        # Load dataframe and extract channel IDs
        df = pd.read_csv("unique.csv")
        channel_seed_ids = df["channel_id"].tolist()
        
        # Expand seed
        expanded = []
        for channel_seed_id in channel_seed_ids:
            print(f"Analyzing channel: {channel_seed_id}")
            discovered_channels = simulate_recommendation_engine(youtube_client, channel_seed_id)
            for ch_id, title in discovered_channels:
                expanded.append([channel_seed_id, ch_id, title])

        # Save results to CSV
        df = pd.DataFrame(expanded, columns=["channel_seed_id", "channel_id", "channel_title"])
        df.to_csv("expanded_seed.csv", index=False)

    if args.crawl:
        print("=="*60)
        print("Crawl")
        print("=="*60)
        crawl(youtube_client)

    if args.is_vn_channel:
        print("=="*60)
        print("Check if Vietnamese Channel")
        print("=="*60)
        is_vn_channels(youtube_client)

    if args.track_info:
        print("=="*60)
        print("Track Video Comment Counts")
        print("=="*60)
        
        track_comment_counts(
            key_manager=key_manager, 
            input_path="video_urls.csv",
            ignore_path="ignore.csv",
            output_path="video_stats_final.csv",
            chunk_size=10000,
        )

def main():
    # Initialize the parser
    parser = argparse.ArgumentParser(
        description="A professional CLI tool for data processing."
    )
    
    # Add arguments
    parser.add_argument("--create_seed", default=False, action="store_true", help="Generate initial seed data")
    parser.add_argument("--expand_seed", default=False, action="store_true", help="Expand data based on seed channels")
    parser.add_argument("--crawl", default=False, action="store_true", help="Start crawling process")
    parser.add_argument("--is_vn_channel", default=False, action="store_true", help="Filter for Vietnamese channels")
    parser.add_argument("--track_info", default=False, action="store_true", help="Track video statistics and comments")

    # Parse arguments
    args = parser.parse_args()

    # Pass arguments to process_parser
    process_parser(args)

if __name__ == "__main__":
    main()
