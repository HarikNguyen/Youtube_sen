import os
from googleapiclient.errors import HttpError

def expand_by_topic_id(youtube_client, topic_id, max_results=10):
    """
    Strategy 1 Sub-routine: Searches for VN channels sharing a specific Freebase Topic ID.
    """
    discovered_channels = set()
    try:
        search_request = youtube_client.search().list(
            part="snippet",
            type="channel",
            regionCode="VN",       # Strictly filter for Vietnam
            topicId=topic_id,      # Use the algorithmic topic ID
            maxResults=max_results 
        )
        search_response = search_request.execute()
        
        for item in search_response.get("items", []):
            channel_id = item["snippet"]["channelId"]
            channel_title = item["snippet"]["title"]
            discovered_channels.add((channel_id, channel_title))
            
    except HttpError as e:
        print(f"[!] Error searching by topic ID {topic_id}: {e}")
        
    return discovered_channels

def get_channel_subscriptions(youtube_client, channel_id, max_results=50):
    """
    Strategy 2 Sub-routine: Fetches the list of channels the seed channel subscribes to.
    """
    subscribed_channels = set()
    try:
        sub_request = youtube_client.subscriptions().list(
            part="snippet",
            channelId=channel_id,
            maxResults=max_results
        )
        sub_response = sub_request.execute()
        
        for item in sub_response.get("items", []):
            sub_channel_id = item["snippet"]["resourceId"]["channelId"]
            sub_channel_title = item["snippet"]["title"]
            subscribed_channels.add((sub_channel_id, sub_channel_title))
            
    except HttpError as e:
        # A 403 error means the channel's subscription list is private
        if e.resp.status == 403:
             print("[i] Subscriptions are private. Skipping Strategy 2.")
        else:
             print(f"[!] Error retrieving subscriptions: {e}")
             
    return subscribed_channels

def simulate_recommendation_engine(youtube_client, seed_channel_id):
    """
    Master function combining Strategy 1 (Topic Engine) and Strategy 2 (Subscriptions)
    to find related channels for a given seed channel.
    """
    print(f"\nAnalyzing Seed Channel: {seed_channel_id}")
    related_network = set()
    
    # =========================================================
    # STRATEGY 1: REVERSE ENGINEER VIA TOPIC ENGINE
    # =========================================================
    print("==> Executing Strategy 1: Topic Engine Extraction...")
    try:
        channel_request = youtube_client.channels().list(
            part="topicDetails",
            id=seed_channel_id
        )
        channel_response = channel_request.execute()
        
        if not channel_response.get("items"):
            print("[!] Channel not found.")
        else:
            topic_details = channel_response["items"][0].get("topicDetails", {})
            topic_ids = topic_details.get("topicIds", [])
            
            if not topic_ids:
                print("[i] No algorithmic topics assigned to this channel.")
            else:
                for t_id in topic_ids:
                    print(f"* Searching VN channels for Topic ID: {t_id}")
                    # Fetch related channels using the extracted topic ID
                    topic_peers = expand_by_topic_id(youtube_client, t_id)
                    related_network.update(topic_peers)
                    
    except HttpError as e:
        print(f"[!] Error executing Strategy 1: {e}")

    # =========================================================
    # STRATEGY 2: SCAN SUBSCRIPTIONS
    # =========================================================
    print("==> Executing Strategy 2: Subscription Network Scan...")
    subs = get_channel_subscriptions(youtube_client, seed_channel_id)
    if subs:
        print(f"* Found {len(subs)} public subscriptions.")
        related_network.update(subs)

    # =========================================================
    # CLEANUP & RETURN
    # =========================================================
    # Remove the seed channel itself if it was accidentally captured
    related_network = {ch for ch in related_network if ch[0] != seed_channel_id}
    
    print(f" -> Completed. Total unique related channels found: {len(related_network)}")
    return related_network

from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
youtube = build("youtube", "v3", developerKey=API_KEY)

def main():
    # ID của một kênh mồi (seed channel) tại Việt Nam
    seed_id = "UCZ4zFf9gjnIHP5sutJ3UBnw" 
    
    # Chạy hệ thống mô phỏng để lấy danh sách kênh liên quan
    discovered_channels = simulate_recommendation_engine(youtube, seed_id)
    
    print("\n=== FINAL DISCOVERED NETWORK ===")
    for ch_id, title in discovered_channels:
         print(f"{title} - {ch_id}")

if __name__ == "__main__":
    main()
