import pandas as pd
import yt_dlp
import time
import random


def expand_youtube_channels_by_tags():
    file_data = pd.read_csv("unique.csv")
    channels_list = file_data.to_dict("records")

    known_channels = set()
    for channel in channels_list:
        known_channels.add(str(channel["channel_id"]))

    main_proxy = (
        "http://pcVyRk0aXV-res-vn:PC_4Ywuqw5KzJEOLJJwM@proxy-us.proxy-cheap.com:5959"
    )

    # Settings for fast scanning (used for channels and searching)
    fast_settings = {
        "extract_flat": True,
        "quiet": True,
        "no_warnings": True,
        "proxy": main_proxy,
        "cookiefile": "cookies.txt",
        "ignoreerrors": True,
        "ignore_no_formats_error": True,
    }

    # Settings for reading deep video information (to get tags)
    deep_video_settings = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": False,
        "skip_download": True,
        "proxy": main_proxy,
        "cookiefile": "cookies.txt",
        "ignoreerrors": True,
        "ignore_no_formats_error": True,
    }

    current_index = 0
    max_channels_limit = 10000

    while (
        current_index < len(channels_list) and len(channels_list) < max_channels_limit
    ):

        current_channel = str(channels_list[current_index]["channel_id"])
        current_category = channels_list[current_index]["category"]

        print("Checking channel index " + str(current_index) + ": " + current_channel)

        wait_time = random.uniform(2.0, 4.0)
        print("Resting for " + str(round(wait_time, 1)) + " seconds.")
        time.sleep(wait_time)

        # Step 1: Get the newest video from the channel
        channel_url = "https://www.youtube.com/channel/" + current_channel + "/videos"
        target_video_url = ""

        fast_settings["playlistend"] = 1  # We only need 1 video to get tags

        with yt_dlp.YoutubeDL(fast_settings) as ydl:
            channel_info = ydl.extract_info(channel_url, download=False)
            if channel_info and "entries" in channel_info:
                for video in channel_info["entries"]:
                    if video and "url" in video:
                        target_video_url = video["url"]
                        break

        # Step 2: Read the tags from that video
        video_tags = []
        if target_video_url != "":
            print("Reading tags from video: " + target_video_url)
            with yt_dlp.YoutubeDL(deep_video_settings) as ydl:
                video_info = ydl.extract_info(target_video_url, download=False)

                if (
                    video_info
                    and "tags" in video_info
                    and video_info["tags"] is not None
                ):
                    # Take up to 4 tags to avoid searching too much
                    video_tags = video_info["tags"][:4]

        # Step 3: Search YouTube using those tags to find new channels
        fast_settings.pop("playlistend", None)  # Remove playlist limit for searching

        for tag in video_tags:
            if len(channels_list) >= max_channels_limit:
                break

            print("Searching YouTube for keyword: " + tag)

            # ytsearch3 means we only take the top 3 results from the search page
            search_query = "ytsearch3:" + tag

            with yt_dlp.YoutubeDL(fast_settings) as ydl:
                search_results = ydl.extract_info(search_query, download=False)
                if search_results and "entries" in search_results:
                    for result in search_results["entries"]:
                        new_channel_id = result.get("channel_id")
                        new_channel_name = result.get("channel")

                        if new_channel_id and new_channel_id not in known_channels:
                            known_channels.add(new_channel_id)

                            channels_list.append(
                                {
                                    "channel_id": new_channel_id,
                                    "title": new_channel_name,
                                    "category": current_category,
                                }
                            )
                            print("Found new related channel: " + str(new_channel_name))

        current_index += 1

        progress_data = pd.DataFrame(channels_list)
        progress_data.to_csv("expanded.csv", index=False)
        print("Progress saved. Total channels so far: " + str(len(channels_list)))
        print(" ")

    print("Process stopped. Final save completed.")


if __name__ == "__main__":
    expand_youtube_channels_by_tags()
