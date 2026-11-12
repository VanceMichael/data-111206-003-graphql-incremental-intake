import json
from urllib.request import urlopen


def read_plain(url):
    with urlopen(url, timeout=5) as response:
        if response.headers.get_content_type() != 'application/json':
            raise ValueError('当前入口只读取普通 JSON')
        body = response.read(1048577)
        if len(body) > 1048576:
            raise ValueError('响应过大')
        return body, json.loads(body)
