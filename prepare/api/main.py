import os
import argparse
import json
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build
from search_cate import create_seed
from expand_seed import simulate_recommendation_engine
from crawl import crawl

load_dotenv()
API_KEY = os.getenv("API_KEY")

def process_parser(args):
    youtube_client = build("youtube", "v3", developerKey=API_KEY)
    if args.create_seed:
        print("=="*60)
        print("Create seed")
        print("=="*60)
        seed = create_seed(youtube_client)
        with open("seed.json", "w") as f:
            json.dump(seed, f, indent=4)

    if args.expand_seed:
        print("=="*60)
        print("Expand seed")
        print("=="*60)
        # load df and get channel id
        df = pd.read_csv("unique.csv")
        channel_seed_ids = df["channel_id"].tolist()
        # expand seed
        expanded = []
        for channel_seed_id in channel_seed_ids:
            print(f"Analyzing channel: {channel_seed_id}")
            discovered_channels = simulate_recommendation_engine(youtube_client, channel_seed_id)
            for ch_id, title in discovered_channels:
                expanded.append([channel_seed_id, ch_id, title])

        # save to csv
        df = pd.DataFrame(expanded, columns=["channel_seed_id", "channel_id", "channel_title"])
        df.to_csv("expanded_seed.csv", index=False)

    if args.crawl:
        print("=="*60)
        print("Crawl")
        print("=="*60)
        crawl(youtube_client)

def main():
    # Initialize the parser
    parser = argparse.ArgumentParser(
        description="A professional CLI tool for data processing."
    )
    
    # Add arguments
    parser.add_argument("--create_seed", default=False, action="store_true")
    parser.add_argument("--expand_seed", default=False, action="store_true")
    parser.add_argument("--crawl", default=False, action="store_true")

    # Parse arguments
    args = parser.parse_args()

    # Pass arguments to process_parser
    process_parser(args)

if __name__ == "__main__":
    main()
