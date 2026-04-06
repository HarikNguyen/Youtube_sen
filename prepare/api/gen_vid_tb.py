import os
import pandas as pd
import isodate
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")


# ==========================================
# CONFIGURATION
# ==========================================
INPUT_FILE = 'expanded.csv'
OUTPUT_FILE = 'videos.csv'

youtube = build('youtube', 'v3', developerKey=API_KEY)

# ==========================================
# CORE FUNCTIONS
# ==========================================

def get_uploads_playlist_id(channel_id):
    """Retrieves the system-generated 'Uploads' playlist for any channel."""
    try:
        request = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        )
        response = request.execute()
        if response.get('items'):
            return response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    except HttpError as e:
        print(f"Error accessing channel {channel_id}: {e}")
    return None

def fetch_all_video_ids(playlist_id):
    """Paginates through the playlist to gather all video IDs."""
    video_ids = []
    next_page_token = None

    while True:
        try:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            for item in response.get('items', []):
                video_ids.append(item['contentDetails']['videoId'])

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        except HttpError as e:
            print(f"Error fetching playlist items: {e}")
            break
            
    return video_ids

def classify_video_type(video_item):
    """
    Logic to determine video category based on available metadata.
    Priority: Livestream > Short (<= 60s) > Static
    """
    # Check for livestream indicators
    if 'liveStreamingDetails' in video_item:
        return 'livestream'
    
    # Check duration for Shorts classification
    duration_iso = video_item.get('contentDetails', {}).get('duration', 'PT0S')
    try:
        duration_seconds = isodate.parse_duration(duration_iso).total_seconds()
        if duration_seconds <= 60:
            return 'short'
    except Exception:
        pass
        
    return 'static'

def get_video_details_and_type(video_ids):
    """Fetches details in batches of 50 to minimize API quota usage."""
    results = {}
    
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]
        try:
            request = youtube.videos().list(
                part="contentDetails,liveStreamingDetails",
                id=','.join(chunk)
            )
            response = request.execute()
            
            for item in response.get('items', []):
                results[item['id']] = classify_video_type(item)
                
        except HttpError as e:
            print(f"Error fetching batch details: {e}")
            
    return results

# ==========================================
# MAIN PROCESS
# ==========================================

def run_data_collection():
    if not os.path.exists(INPUT_FILE):
        print(f"Source file {INPUT_FILE} not found.")
        return

    # Load input data
    input_df = pd.read_csv(INPUT_FILE)
    
    # Determine if we need to write the header for the output file
    is_first_write = not os.path.exists(OUTPUT_FILE)

    for index, row in input_df.iterrows():
        channel_id = row['channel_id']
        channel_name = row.get('name', 'Unknown')
        
        print(f"[{index + 1}/{len(input_df)}] Processing: {channel_name} ({channel_id})")
        
        uploads_id = get_uploads_playlist_id(channel_id)
        if not uploads_id:
            continue
            
        all_ids = fetch_all_video_ids(uploads_id)
        if not all_ids:
            print(f"   - No videos found.")
            continue
            
        print(f"   - Found {len(all_ids)} videos. Classifying types...")
        type_mapping = get_video_details_and_type(all_ids)
        
        # Prepare data for CSV export
        batch_data = []
        for vid in all_ids:
            batch_data.append({
                'category': row['category'],
                'name': row['name'],
                'channel_id': channel_id,
                'title': row['title'],
                'video_id': vid,
                'type': type_mapping.get(vid, 'static')
            })
            
        # Append to CSV immediately to prevent data loss
        output_df = pd.DataFrame(batch_data)
        output_df.to_csv(OUTPUT_FILE, mode='a', header=is_first_write, index=False, encoding='utf-8-sig')
        
        is_first_write = False
        print(f"   - Successfully saved {len(batch_data)} records.")

if __name__ == "__main__":
    run_data_collection()
