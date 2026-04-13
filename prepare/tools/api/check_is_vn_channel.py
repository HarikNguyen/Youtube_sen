import re
import os
import csv
from googleapiclient.discovery import build

# Replace with your actual YouTube Data API Key
def extract_channel_ids(file_path):
    """
    Reads the file and extracts Channel IDs using Regex.
    Format expected: ID: UCl-Wf44-szOK5QjSmQ2KCug | Name: VTV3
    """
    channel_ids = []
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return []

    # Regex to find ID: [Any Alphanumeric/Underscore/Dash]
    id_pattern = re.compile(r"ID:\s*([a-zA-Z0-9_-]+)")

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            match = id_pattern.search(line)
            if match:
                channel_ids.append(match.group(1))
    
    return channel_ids

def is_vietnamese_text(text):
    """Checks for Vietnamese specific characters."""
    if not text: return False
    vietnamese_re = re.compile(
        r'[àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệđìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵ]',
        re.IGNORECASE
    )
    return bool(vietnamese_re.search(text))

def check_vietnamese_channels(yt_cli, channel_ids):
    results = []

    # Process in batches of 50 (YouTube API limit per request)
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i+50]
        request = yt_cli.channels().list(
            part="snippet,brandingSettings",
            id=','.join(batch)
        )
        response = request.execute()

        for item in response.get('items', []):
            channel_id = item.get('id')
            snippet = item.get('snippet', {})
            branding = item.get('brandingSettings', {}).get('channel', {})
            
            title = snippet.get('title', '')
            description = snippet.get('description', '')
            country = branding.get('country', '')
            
            # Logic to determine if it's Vietnamese
            is_vn = False
            if country == 'VN':
                is_vn = True
            elif is_vietnamese_text(title) or is_vietnamese_text(description):
                is_vn = True
            
            results.append({
                "id": channel_id,
                "title": title,
                "is_vn": is_vn,
                "country": country if country else "N/A"
            })
            
    return results

def is_vn_channels(yt_cli, file_name='m.txt', res_file='nvn_filtered.csv'):
    
    print(f"--- Processing {file_name} ---")
    
    # Step 1: Get IDs from file
    ids = extract_channel_ids(file_name)
    if not ids:
        return

    print(f"Found {len(ids)} channel IDs. Checking with YouTube API...")

    # Step 2: Check with API
    results = check_vietnamese_channels(yt_cli, ids)

    # Step 3: Print results & save to CSV
    res = []
    print(f"\n{'ID':<24} | {'VN?':<5} | {'Country':<8} | {'Title'}")
    print("-" * 80)
    for r in results:
        vn_status = "YES" if r['is_vn'] else "NO"
        if not r['is_vn']:
            res.append(r)
        print(f"{r['id']:<24} | {vn_status:<5} | {r['country']:<8} | {r['title']}")

    print("-" * 80)
    print(f"Found {len(res)} does not Vietnamese channels")

    # Save to CSV
    with open(res_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['id', 'title', 'is_vn', 'country']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(res)
