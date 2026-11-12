import json
from pathlib import Path
from urllib.request import urlopen

from incremental import IncrementalCollector, ReceiveFailure, extract_boundary


def read_plain(url):
    with urlopen(url, timeout=5) as response:
        if response.headers.get_content_type() != 'application/json':
            raise ValueError('当前入口只读取普通 JSON')
        body = response.read(1048577)
        if len(body) > 1048576:
            raise ValueError('响应过大')
        return body, json.loads(body)


def load_protocol(path='fixtures/protocol.json'):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def read_incremental(url, accept_errors=(), chunk_size=4096, protocol=None, cancel_after=None):
    """流式接收 multipart/mixed 增量响应，与 read_plain 并存的正式采集通道。

    按块读取来源，分片一完整就交给 IncrementalCollector 处理，不把全部
    字节攒到末尾再解析。accept_errors 按字段路径声明可接受的错误。
    cancel_after 用于在处理指定数量的分片后取消采集（取消会终止来源
    读取）。返回 outcome（普通查询视角）、snapshot（诊断视角）与原文。
    """
    protocol = protocol or load_protocol()
    collector = IncrementalCollector(protocol, accept_errors)
    raw = bytearray()
    try:
        with urlopen(url, timeout=5) as response:
            boundary = extract_boundary(response.headers.get('Content-Type', ''))
            expected = protocol.get('boundary')
            if expected is not None and boundary != expected.encode('ascii'):
                raise ReceiveFailure('boundary 缺失或与协议不符', boundary.decode('ascii', 'replace'))
            collector.begin(boundary)
            read_chunk = getattr(response, 'read1', None) or response.read
            while True:
                chunk = read_chunk(chunk_size)
                if not chunk:
                    break
                collector.feed(chunk)
                raw += chunk
                if cancel_after is not None and collector.parts_seen >= cancel_after:
                    collector.cancel()
                    break
            collector.finish()
    except ReceiveFailure as exc:
        if collector.failure is None and not collector.cancelled:
            collector.failure = exc
    return {'outcome': collector.outcome(), 'snapshot': collector.snapshot(), 'raw': bytes(raw)}
