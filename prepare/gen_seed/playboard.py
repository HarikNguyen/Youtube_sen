import requests
import json
import pandas as pd
from cursor import get_root_cursor

# The base URL
URL = "https://lapi.playboard.co/v1/chart/channel"

# Query parameters extracted from the URL
PARAMS = {
    "locale": "en",
    "countryCode": "VN",
    "period": "1775174400",
    "cursor": "",
    "size": "20",
    "chartTypeId": "10",
    "periodTypeId": "2",
    "indexDimensionId": "",
    "indexTypeId": "",
    "indexTarget": "",
    "indexCountryCode": "VN",
}


def get_params(
    cursor="fad5c654d6fd843ef2e216058378ada3:d57f39fc7271f0a8fe995cbebabe8dfb90b6e3b62225639ff503043558d8b066422cc6ee3db59fbbc604e26d6979859e3e10b2705a60c5d236866a88ff9bc85f50b8c833661f6c8f2120c2262f009d1e",
    index_dimensionid="10",
    index_typeid="4",
    index_target="19",
):
    params = PARAMS.copy()
    params["indexDimensionId"] = index_dimensionid
    params["indexTypeId"] = index_typeid
    params["indexTarget"] = index_target
    return params


def flat_params(params):
    flat_str = "?"
    for key, value in params.items():
        if value != "":
            flat_str += f"&{key}={value}"
    return flat_str


# Headers to mimic the browser request
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "vi,en-US;q=0.9,en;q=0.8",
    "origin": "https://playboard.co",
    "priority": "u=1, i",
    "referer": "https://playboard.co/",
    "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
}

CATEGORIES = [
    {
        "index_dimensionid": "20",
        "index_typeid": "3",
        "index_target": "cover-dance",
    },
    {"index_dimensionid": "20", "index_typeid": "3", "index_target": "mukbang"},
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "15",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "10",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "20",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "25",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "22",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "19",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "18",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "17",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "2",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "23",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "24",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "1",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "26",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "27",
    },
    {
        "index_dimensionid": "20",
        "index_typeid": "4",
        "index_target": "28",
    },
]


def single_crawl(
    params_flatted,
    authorization="Bearer 8ER5JzEblpqSTdyFrmpKjQUqXqyTvW0SrQN-i_VJfLPomkINVs1lA9vGK3EuF4AOthmKzFjqHeltpvVlaNUQqxp8_QZDTwUxNlb6qO3xmkPtQIYBZKk7vd-rdfYZlede",
):
    url = f"{URL}{params_flatted}"

    headers = HEADERS.copy()
    headers["authorization"] = authorization
    response = requests.request("GET", url, headers=HEADERS)

    return response


def crawl_all(
    authorization="Bearer 8ER5JzEblpqSTdyFrmpKjQUqXqyTvW0SrQN-i_VJfLPomkINVs1lA9vGK3EuF4AOthmKzFjqHeltpvVlaNUQqxp8_QZDTwUxNlb6qO3xmkPtQIYBZKk7vd-rdfYZlede",
):
    data = []
    for cate_dict in CATEGORIES:
        data.append([])
        try:
            while True:
                cursor = get_root_cursor()
                cate_dict["cursor"] = cursor
                params_flatted = flat_params(get_params(**cate_dict))
                response = single_crawl(params_flatted, authorization)
                next_cursor, res_data = process_to_next(response)
                data[-1].extend(res_data)
                cursor = next_cursor
        except Exception as e:
            print("Error or End. But it's logged: ", e)
            continue

    return data


def process_to_next(response):
    json_data = response.json()
    next_cursor = json_data["cursor"]
    data = json_data["list"]
    return next_cursor, data


def to_csv(lol_data):
    """
    Convert list of list to csv
    """
    # get channel_id + channel_name
    print(len(lol_data))
    filtered_data = []
    for category_index, category_data in enumerate(lol_data):
        for channel_data in category_data:
            channel_id = channel_data["channel"]["channelId"]
            channel_name = channel_data["channel"]["name"]
            filtered_data.append([category_index, channel_id, channel_name])

    # save to csv
    df = pd.DataFrame(
        filtered_data, columns=["category_index", "channel_id", "channel_name"]
    )
    df.to_csv("playboard.csv", index=False)


def main():
    lol_data = crawl_all("Bearer 8rcw4U-eKGV8AOZra3Nmm-6zBg6zQPRHI0zVra1R2t7TYwCj3MW-7sdq1I_iDogrqKRKLsLOibWeo8vGnipRtXCM8LDVtVT6IUXFJzpXjyJzt1ixzQT-VHLLGbbvKPuU")
    to_csv(lol_data)


if __name__ == "__main__":
    main()
