import pandas as pd
import scrapetube
import csv

# List of your channel IDs

df = pd.read_csv('expanded.csv')
CHANNEL_IDS = df['channel_id'].tolist()

def collect_all_videos(channel_ids, output_file):
    base_url = "https://www.youtube.com/watch?v="
    
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(['channel_id', 'video_type', 'video_url'])

        for channel_id in channel_ids:
            print(f"Processing channel: {channel_id}")
            
            # Content types to fetch
            types = ['videos', 'shorts', 'streams']
            
            for content_type in types:
                try:
                    # Fetching video data from YouTube's internal API
                    videos = scrapetube.get_channel(channel_id, content_type=content_type)
                    
                    count = 0
                    for video in videos:
                        video_id = video['videoId']
                        writer.writerow([channel_id, content_type, f"{base_url}{video_id}"])
                        count += 1
                    
                    print(f"Collected {count} {content_type}")
                
                except Exception as e:
                    print(f"Error fetching {content_type} for {channel_id}: {e}")
            

if __name__ == "__main__":
    collect_all_videos(CHANNEL_IDS, 'res.csv')
