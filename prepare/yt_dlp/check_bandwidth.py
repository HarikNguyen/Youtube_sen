import yt_dlp
import json
import sys

def check_bandwidth_usage():
    # Setup standard settings
    channel_settings = {
        'extract_flat': True,
        'playlistend': 3,
        'quiet': True,
        'no_warnings': True,
    }

    # Use one channel to test
    test_channel_url = "https://www.youtube.com/@HuyPhanVlog/videos"
    
    print("Starting the download test...")
    
    with yt_dlp.YoutubeDL(channel_settings) as ydl:
        # Step 1: Download the information
        channel_info = ydl.extract_info(test_channel_url, download=False)
        
        # Step 2: Turn the information into standard text
        text_data = json.dumps(channel_info)
        
        # Step 3: Check how heavy the text is in bytes
        size_in_bytes = sys.getsizeof(text_data)
        
        # Step 4: Convert bytes to Kilobytes
        size_in_kb = size_in_bytes / 1024
        
        print("One channel request used this many Kilobytes: " + str(round(size_in_kb, 2)))
        
        # Step 5: Estimate for the whole project (example: 5000 channels)
        number_of_channels_to_scan = 5000
        total_kb_needed = size_in_kb * number_of_channels_to_scan
        total_megabytes_needed = total_kb_needed / 1024
        
        print("Estimate for 5000 channels in Megabytes: " + str(round(total_megabytes_needed, 2)))

if __name__ == "__main__":
    check_bandwidth_usage()
