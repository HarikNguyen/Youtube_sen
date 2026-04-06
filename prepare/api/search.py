def get_all_videos(channel_id, youtube_client):

    uploads_playlist_id = "UU" + channel_id[2:]

    video_list = []
    next_page_token = None

    print(f"Crawl playlist: {uploads_playlist_id}")

    while True:
        request = youtube_client.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=next_page_token,
        )
        response = request.execute()

        for item in response.get("items", []):
            video_data = {
                "title": item["snippet"]["title"],
                "video_id": item["contentDetails"]["videoId"],
                "published_at": item["snippet"]["publishedAt"],
            }
            video_list.append(video_data)

        next_page_token = response.get("nextPageToken")

        print(f"Crawll {len(video_list)} videos...")

        if not next_page_token:
            break

    return video_list
