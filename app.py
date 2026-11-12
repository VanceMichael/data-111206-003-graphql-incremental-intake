import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from raw_store import RawStore


def demo(store):
    observed_at = datetime.now(timezone.utc).isoformat()
    from client import read_plain
    from source import serve
    from urllib.request import urlopen
    with serve() as base:
        raw, parsed = read_plain(base+'/plain')
        plain_digest = store.save('directory:plain', raw, {'content_type': 'application/json'}, observed_at)
        with urlopen(base+'/incremental',timeout=5) as response:
            raw = response.read()
            content_type = response.headers['Content-Type']
        mixed_digest = store.save('directory:incremental', raw, {'content_type': content_type}, observed_at)
    return {'普通响应': parsed, '原文摘要': [plain_digest, mixed_digest], '分片原文字节数': len(raw), '说明': '分片响应只留原文，不发布企业档案'}


def main():
    parser = argparse.ArgumentParser(description='本地原文采集演示')
    parser.add_argument('command', choices=['demo'])
    parser.add_argument('--output', default='output/raw.sqlite3')
    args = parser.parse_args()
    store = RawStore(args.output)
    try:
        result = demo(store)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        store.close()


if __name__ == '__main__':
    main()
