import os
import json
import pandas as pd
from googleapiclient.errors import HttpError
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import JSONFormatter

formatter = JSONFormatter()


def get_uploads_playlist_id(youtube_client, channel_id):
    """Retrieves the ID of the channel's default 'Uploads' playlist."""
    try:
        request = youtube_client.channels().list(part="contentDetails", id=channel_id)
        response = request.execute()
        if response["items"]:
            return response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    except HttpError as e:
        print(f"Error fetching channel {channel_id}: {e}")
    return None


def get_all_videos_from_playlist(youtube_client, playlist_id):
    """Retrieves all video IDs from a given playlist, handling pagination."""
    video_ids = []
    next_page_token = None

    while True:
        try:
            request = youtube_client.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token,
            )
            response = request.execute()

            for item in response.get("items", []):
                video_ids.append(item["contentDetails"]["videoId"])

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
        except HttpError as e:
            print(f"Error fetching playlist {playlist_id}: {e}")
            break

    return video_ids


def get_video_metadata_batched(youtube_client, video_ids):
    """Fetches metadata for videos in batches of 50 to save API quota."""
    metadata_dict = {}

    # Process in chunks of 50 (API limit for a single request)
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i : i + 50]
        try:
            request = youtube_client.videos().list(
                part="snippet,statistics,contentDetails", id=",".join(chunk)
            )
            response = request.execute()

            for item in response.get("items", []):
                metadata_dict[item["id"]] = item
        except HttpError as e:
            print(f"Error fetching metadata for chunk: {e}")

    return metadata_dict


def get_transcript(video_id):
    """Scrapes the transcript without using YouTube API quota."""
    try:
        transcript = YouTubeTranscriptApi.get_transcript(
            video_id, languages=["vi", "en"]
        )
        return transcript
    except Exception as e:
        # Many videos won't have transcripts, this is normal.
        return None


def get_all_comments(youtube_client, video_id):
    """Retrieves all parent and child comments for a video."""
    comments_data = []
    next_page_token = None

    while True:
        try:
            request = youtube_client.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token,
                textFormat="plainText",
            )
            response = request.execute()

            for item in response.get("items", []):
                # Extract parent comment
                top_comment = item["snippet"]["topLevelComment"]["snippet"]
                thread_id = item["id"]

                comment_obj = {
                    "comment_id": thread_id,
                    "author": top_comment.get("authorDisplayName"),
                    "text": top_comment.get("textDisplay"),
                    "like_count": top_comment.get("likeCount"),
                    "published_at": top_comment.get("publishedAt"),
                    "replies": [],
                }

                # Extract replies if they exist in the initial response
                if "replies" in item:
                    for reply in item["replies"]["comments"]:
                        reply_snippet = reply["snippet"]
                        comment_obj["replies"].append(
                            {
                                "reply_id": reply["id"],
                                "author": reply_snippet.get("authorDisplayName"),
                                "text": reply_snippet.get("textDisplay"),
                                "like_count": reply_snippet.get("likeCount"),
                                "published_at": reply_snippet.get("publishedAt"),
                            }
                        )

                # Note: If a comment has > 5 replies, we would theoretically need
                # to call youtube.comments().list(parentId=thread_id).
                # Omitted here to prevent aggressive quota drain, but can be added if needed.

                comments_data.append(comment_obj)

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        except HttpError as e:
            # Comments might be disabled (403 Error)
            print(
                f"Cannot fetch comments for {video_id}. It might be disabled. Error: {e.reason}"
            )
            break

    return comments_data


# ==========================================
# MAIN EXECUTION
# ==========================================


def crawl(youtube_client, file_in="expanded.csv", output_dir="youtube_data_results"):

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    CHANNEL_IDS = []
    df = pd.read_csv(file_in)
    CHANNEL_IDS = df["channel_id"].tolist()

    for channel_id in CHANNEL_IDS:
        print(f"--- Processing Channel: {channel_id} ---")

        # 1. Get the uploads playlist ID
        uploads_playlist_id = get_uploads_playlist_id(youtube_client, channel_id)
        if not uploads_playlist_id:
            continue

        # 2. Get all video IDs for this channel
        video_ids = get_all_videos_from_playlist(youtube_client, uploads_playlist_id)
        print(f"Found {len(video_ids)} videos for channel {channel_id}")

        # 3. Get metadata for all videos efficiently
        video_metadata = get_video_metadata_batched(youtube_client, video_ids)

        # 4. Iterate over each video to get transcript and comments, then save state
        for idx, vid in enumerate(video_ids):
            print(f"Processing video {idx+1}/{len(video_ids)}: {vid}")

            result_data = {
                "video_id": vid,
                "metadata": video_metadata.get(vid, {}),
                "transcript": get_transcript(vid),
                "comments": get_all_comments(youtube_client, vid),
            }

            # Save to disk immediately after processing each video.
            # This is crucial for long-running scripts on Colab/Kaggle.
            file_path = os.path.join(output_dir, f"{channel_id}_{vid}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(result_data, f, ensure_ascii=False, indent=4)

    print("Data collection completed!")
