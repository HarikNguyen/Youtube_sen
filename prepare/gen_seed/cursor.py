import requests
import re

__ROOT_URL = "https://playboard.co/en/youtube-ranking/most-viewed-mukbang-channels-in-viet-nam-daily"

__HEADERS_ROOT = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "vi,en-US;q=0.9,en;q=0.8",
    "cache-control": "max-age=0",
    "priority": "u=0, i",
    "referer": "https://accounts.google.com/",
    "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Cookie": "utcOffset=+07:00; tz=Asia/Saigon; _ga=GA1.1.1117974735.1775237732; AUTR=Nka1zTuift8d7cESC5F6r-pB05JljKhOfd5Hfymv6o-E1dZlg2_pzLv50DuiFv7O; FCCDCF=%5Bnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%22efeeb9ac-8f4b-4631-93e4-6f8a5aeaa409%5C%22%2C%5B1775238809%2C890000000%5D%5D%22%5D%5D%5D; FCNEC=%5B%5B%22AKsRol_-XHmgRwwb_iza6Kj9PKJJLYmFJFyv8_VbFa6IEi9Y5Pgg5gxWQYy47u3JMGI6cerV-5fzK1AJ7QWVQUGcMe2BNU7UI73y_mj_KAZGJjBkQFil7SB-XA6Vg7onijfGrZVagSToOechQpeDqcNT2aFqz6GVQw%3D%3D%22%5D%5D; __gads=ID=e3100c617fdb0155:T=1775238808:RT=1775278792:S=ALNI_Mb751O_IZOok6r5ZXre8IMkkfq5BA; __gpi=UID=0000135af021c967:T=1775238808:RT=1775278792:S=ALNI_Mbuj_40cSBpec07Y2gD9E2k0tqT3w; __eoi=ID=4275322b6287b8de:T=1775238808:RT=1775278792:S=AA-AfjZBALN_mxMaZKeRokq0VdXw; _ga_M1J3WFM34Q=GS2.1.s1775287695$o4$g1$t1775287869$j57$l0$h0",
}


def __get_html():
    response = requests.request("GET", __ROOT_URL, headers=__HEADERS_ROOT, data={})
    html_doc = response.text

    return html_doc


def __get_root_cursor(html_doc):
    pattern = r'cursor:"([a-f0-9:]+)"'
    match = re.search(pattern, html_doc)
    if match:
        # Get the entire line
        full_line = match.group(0)
        # Extract only the code inside
        cursor_value = match.group(1)

        return cursor_value

    return None


def get_root_cursor():
    html_doc = __get_html()
    cursor = __get_root_cursor(html_doc)
    return cursor
