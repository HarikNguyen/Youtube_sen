import os
from googleapiclient.errors import HttpError


def get_vn_categories(youtube_client):
    """Fetches all assignable YouTube video categories for Vietnam."""
    print("Fetching YouTube categories for region: VN...")
    try:
        request = youtube_client.videoCategories().list(
            part="snippet",
            regionCode="VN"
        )
        response = request.execute()
        
        categories = {}
        for item in response.get("items", []):
            # Only keep categories where users can actually upload videos
            if item["snippet"]["assignable"]:
                cat_id = item["id"]
                cat_title = item["snippet"]["title"]
                categories[cat_id] = cat_title
                
        print(f"Found {len(categories)} assignable categories.")
        return categories
    except HttpError as e:
        print(f"An HTTP error occurred: {e}")
        return {}

def get_seed_channels_by_category(youtube_client, category_id, category_title, max_pages=10):
    """Fetches multiple pages of popular videos to extract more unique channels."""
    print(f"Collecting seed channels for: {category_title}")
    unique_channels = set()
    next_page_token = None
    
    try:
        for _ in range(max_pages):
            request = youtube_client.videos().list(
                part="snippet",
                chart="mostPopular",
                regionCode="VN",
                videoCategoryId=category_id,
                maxResults=50,
                pageToken=next_page_token  # Use the token for the next page
            )
            response = request.execute()
            
            for item in response.get("items", []):
                channel_id = item["snippet"]["channelId"]
                channel_title = item["snippet"]["channelTitle"]
                unique_channels.add((channel_id, channel_title))
            
            # Check if there is another page; if not, stop looping
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
                
        return unique_channels
        
    except HttpError as e:
        print(f"Error fetching videos for category {category_title}: {e}")
        return set()

def create_seed(youtube_client):
    # Phase 1: Get Categories
    vn_categories = get_vn_categories(youtube_client)
    
    if not vn_categories:
        print("No categories found or API error. Exiting.")
        return

    # Dictionary to store our final seed list mapping Category -> Channels
    domain_seed_data = {}

    # Phase 2: Get initial seed channels for each category
    print("\nStarting channel extraction per category...")
    for cat_id, cat_title in vn_categories.items():
        channels = get_seed_channels_by_category(youtube_client, cat_id, cat_title)
        domain_seed_data[cat_title] = channels
        
    # Output the results
    print("\n=== FINAL SEED EXTRACTION SUMMARY ===")
    total_channels = 0
    for domain, channels in domain_seed_data.items():
        count = len(channels)
        total_channels += count
        print(f"{domain}: {count} unique channels identified in top 1k trending.")
        
    print(f"\nTotal initial seed channels collected across all domains: {total_channels}")
    
    return to_json_type(domain_seed_data)

def to_json_type(data):
    """
    Recursively converts sets and tuples into lists to ensure 
    the data structure is JSON serializable.
    """
    if isinstance(data, dict):
        return {k: to_json_type(v) for k, v in data.items()}
    elif isinstance(data, (list, set, tuple)):
        return [to_json_type(item) for item in data]
    return data
