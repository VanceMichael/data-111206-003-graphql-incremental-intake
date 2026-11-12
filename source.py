import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


def response_bytes(scenario='normal'):
    boundary = b'trade-mixed-42'
    parts = json.loads(Path('fixtures/parts.json').read_text())
    if scenario == 'retransmit':
        parts.insert(2, parts[1])
    elif scenario == 'conflict':
        parts.insert(2, {'incremental': [{'id': 'people', 'items': [{'name': '矛盾联系人'}], 'subPath': [1]}], 'hasNext': True})
    elif scenario == 'unknown':
        parts[1]['incremental'][0]['id'] = 'unregistered'
    elif scenario == 'errors':
        parts[2] = {'incremental': [{'id': 'cert', 'data': {'certificates': None}, 'errors': [{'message': '合成权限拒绝', 'path': ['company', 'certificates']}]}], 'completed': [{'id': 'cert'}], 'hasNext': True}
    elif scenario != 'normal':
        raise ValueError('未知响应场景')
    return b''.join(b'--' + boundary + b'\r\nContent-Type: application/json; charset=utf-8\r\n\r\n' + json.dumps(part, ensure_ascii=False).encode() + b'\r\n' for part in parts) + b'--' + boundary + b'--\r\n'


def blocks(body, size):
    if size <= 0:
        raise ValueError('块尺寸必须为正数')
    for offset in range(0, len(body), size):
        yield body[offset:offset + size]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        parsed = urlsplit(self.path)
        options = parse_qs(parsed.query)
        if parsed.path == '/plain':
            body = json.dumps({'data': {'company': {'id': 'local-1'}}}).encode()
            content_type = 'application/json'
        elif parsed.path in ('/incremental', '/truncated'):
            try:
                body = response_bytes(options.get('scenario', ['normal'])[0])
            except ValueError:
                self.send_error(400)
                return
            if parsed.path == '/truncated':
                body = body[:-50]
            content_type = 'multipart/mixed; boundary="trade-mixed-42"'
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        for block in blocks(body, 7):
            try:
                self.wfile.write(block)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                break


@contextmanager
def serve():
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}'
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
