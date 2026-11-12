import json
import unittest
from pathlib import Path

from client import read_incremental
from incremental import IncrementalCollector, ReceiveFailure, extract_boundary
from source import response_bytes, serve

PROTOCOL = json.loads(Path('fixtures/protocol.json').read_text(encoding='utf-8'))
PARTS = json.loads(Path('fixtures/parts.json').read_text(encoding='utf-8'))
BOUNDARY = b'trade-mixed-42'


def encode_parts(parts, boundary=BOUNDARY):
    chunks = []
    for part in parts:
        chunks.append(b'--' + boundary + b'\r\nContent-Type: application/json; charset=utf-8\r\n\r\n')
        chunks.append(json.dumps(part, ensure_ascii=False).encode('utf-8'))
        chunks.append(b'\r\n')
    chunks.append(b'--' + boundary + b'--\r\n')
    return b''.join(chunks)


def collect(body, block=7, accept=(), protocol=PROTOCOL):
    """走正式接收路径，失败也返回收集器本身。"""
    collector = IncrementalCollector(protocol, accept)
    collector.begin(BOUNDARY)
    try:
        for offset in range(0, len(body), block):
            collector.feed(body[offset:offset + block])
        collector.finish()
    except ReceiveFailure:
        pass
    return collector


class BoundaryTests(unittest.TestCase):
    def test_quoted_boundary(self):
        self.assertEqual(extract_boundary('multipart/mixed; boundary="trade-mixed-42"'), BOUNDARY)

    def test_plain_boundary(self):
        self.assertEqual(extract_boundary('multipart/mixed; boundary=trade-mixed-42'), BOUNDARY)

    def test_not_multipart(self):
        with self.assertRaises(ReceiveFailure):
            extract_boundary('application/json')

    def test_missing_boundary(self):
        with self.assertRaises(ReceiveFailure):
            extract_boundary('multipart/mixed')


class CollectorTests(unittest.TestCase):
    def test_published_with_ordered_stream(self):
        collector = collect(response_bytes())
        self.assertEqual(collector.status(), 'published')
        data = collector.outcome()['data']
        # subPath 下标落位，不当作追加顺序：下标 1 的联系人先到，下标 0 的后到
        self.assertEqual([c['name'] for c in data['company']['contacts']],
                         ['虚构联系人甲', '虚构联系人乙'])
        self.assertEqual(data['company']['certificates'], ['合成资质'])
        self.assertEqual(collector.snapshot()['branches'], {'cert': '已关闭', 'people': '已关闭'})

    def test_block_sizes_produce_same_result(self):
        body = response_bytes()
        outcomes = [collect(body, block=size).outcome() for size in (1, 2, 3, 5, 7, 13, 64, 1024, len(body))]
        canonical = {json.dumps(o['data'], ensure_ascii=False, sort_keys=True) for o in outcomes}
        self.assertEqual(len(canonical), 1)
        self.assertTrue(all(o['status'] == 'published' for o in outcomes))

    def test_parts_processed_before_eof(self):
        # 首片完整就应当登记分支：证明不是把字节攒到末尾再解析
        body = response_bytes()
        first_end = body.find(b'--' + BOUNDARY, 10) + len(BOUNDARY) + 4
        collector = IncrementalCollector(PROTOCOL)
        collector.begin(BOUNDARY)
        collector.feed(body[:first_end])
        self.assertIsNotNone(collector.data)
        self.assertEqual(set(collector.branches), {'cert', 'people'})
        self.assertEqual(collector.status(), 'receiving')

    def test_duplicate_retransmit_recognized(self):
        collector = collect(response_bytes('retransmit'))
        self.assertEqual(collector.status(), 'published')
        self.assertEqual(collector.duplicates, 1)
        self.assertEqual([c['name'] for c in collector.outcome()['data']['company']['contacts']],
                         ['虚构联系人甲', '虚构联系人乙'])

    def test_conflict_kept_and_recorded(self):
        collector = collect(response_bytes('conflict'))
        self.assertEqual(collector.status(), 'published')
        contacts = collector.outcome()['data']['company']['contacts']
        self.assertEqual([c['name'] for c in contacts], ['虚构联系人甲', '虚构联系人乙'])
        self.assertEqual(len(collector.conflicts), 1)
        conflict = collector.conflicts[0]
        self.assertEqual(conflict['path'], ['company', 'contacts', 1])
        self.assertEqual(conflict['收到'], {'name': '矛盾联系人'})

    def test_unknown_pending_id(self):
        collector = collect(response_bytes('unknown'))
        self.assertEqual(collector.status(), 'failed')
        self.assertEqual(collector.failure.reason, '未登记的 pending id')

    def test_out_of_range_subpath(self):
        parts = [PARTS[0], {'incremental': [{'id': 'people', 'items': [{'name': '越界'}], 'subPath': [-1]}], 'hasNext': True}]
        collector = collect(encode_parts(parts))
        self.assertEqual(collector.failure.reason, '路径越界')

    def test_unresolvable_pending_path(self):
        parts = [dict(PARTS[0], pending=[{'id': 'bad', 'path': ['company', 'missing']}])]
        collector = collect(encode_parts(parts))
        self.assertEqual(collector.failure.reason, '路径越界')

    def test_truncated_disconnect(self):
        collector = collect(response_bytes()[:-50])
        self.assertEqual(collector.status(), 'failed')
        self.assertEqual(collector.failure.reason, '连接中断')

    def test_missing_end_flag(self):
        parts = json.loads(json.dumps(PARTS))  # 深拷贝后让最后一片仍是 hasNext=true
        parts[-1]['hasNext'] = True
        collector = collect(encode_parts(parts))
        self.assertEqual(collector.failure.reason, '缺少结束标志')

    def test_unclosed_branch(self):
        parts = [PARTS[0], {'incremental': [{'id': 'people', 'items': [{'name': '甲'}], 'subPath': [0]}],
                            'completed': [{'id': 'people'}], 'hasNext': False}]
        collector = collect(encode_parts(parts))
        self.assertEqual(collector.failure.reason, '分支未关闭')
        self.assertEqual(collector.snapshot()['branches']['cert'], '等待中')

    def test_part_size_limit(self):
        protocol = dict(PROTOCOL, max_part_bytes=10)
        collector = collect(response_bytes(), protocol=protocol)
        self.assertEqual(collector.failure.reason, '分片超过尺寸限制')

    def test_body_size_limit(self):
        protocol = dict(PROTOCOL, max_body_bytes=50)
        collector = collect(response_bytes(), protocol=protocol)
        self.assertEqual(collector.failure.reason, '响应体超过尺寸限制')

    def test_invalid_json_part(self):
        body = (b'--' + BOUNDARY + b'\r\nContent-Type: application/json\r\n\r\n{oops\r\n'
                b'--' + BOUNDARY + b'--\r\n')
        collector = collect(body)
        self.assertEqual(collector.failure.reason, '分片不是合法 JSON')

    def test_cancel_keeps_partial_for_diagnostics_only(self):
        body = response_bytes()
        first_end = body.find(b'--' + BOUNDARY, 10) + len(BOUNDARY) + 4
        collector = IncrementalCollector(PROTOCOL)
        collector.begin(BOUNDARY)
        collector.feed(body[:first_end])
        collector.cancel()
        with self.assertRaises(ReceiveFailure):
            collector.feed(body[first_end:first_end + 7])
        collector.finish()
        self.assertEqual(collector.status(), 'cancelled')
        self.assertIsNone(collector.outcome()['data'])  # 普通成功查询读不到
        self.assertIsNotNone(collector.snapshot()['data'])  # 半成品可供诊断

    def test_errors_accepted_publishes_with_null(self):
        collector = collect(response_bytes('errors'), accept=[['company', 'certificates']])
        self.assertEqual(collector.status(), 'published')
        data = collector.outcome()['data']
        self.assertIsNone(data['company']['certificates'])
        self.assertEqual(collector.path_state(['company', 'certificates']), '错误置空')
        self.assertEqual(collector.errors[0]['path'], ['company', 'certificates'])

    def test_errors_not_accepted_withheld(self):
        collector = collect(response_bytes('errors'))
        self.assertEqual(collector.status(), 'withheld')
        self.assertIsNone(collector.outcome()['data'])
        self.assertEqual(collector.outcome()['unmet'][0]['path'], ['company', 'certificates'])

    def test_missing_branch_data_is_not_confused_with_error_null(self):
        parts = [PARTS[0], {'incremental': [{'id': 'people', 'items': [{'name': '甲'}], 'subPath': [0]}],
                            'completed': [{'id': 'people'}, {'id': 'cert'}], 'hasNext': False}]
        collector = collect(encode_parts(parts))
        self.assertEqual(collector.status(), 'withheld')
        # 资质分支从未送达：是“尚未到达”，不是错误置空
        self.assertEqual(collector.path_state(['company', 'certificates']), '尚未到达')
        self.assertEqual(collector.path_state(['company', 'id']), '已到达')


class ClientTests(unittest.TestCase):
    def test_read_incremental_normal(self):
        with serve() as base:
            result = read_incremental(base + '/incremental')
        outcome = result['outcome']
        self.assertEqual(outcome['status'], 'published')
        self.assertEqual(outcome['data']['company']['certificates'], ['合成资质'])
        self.assertEqual(result['raw'], response_bytes())

    def test_chunk_size_matrix_agrees(self):
        with serve() as base:
            results = [read_incremental(base + '/incremental', chunk_size=size) for size in (1, 7, 4096)]
        canonical = {json.dumps(r['outcome']['data'], ensure_ascii=False, sort_keys=True) for r in results}
        self.assertEqual(len(canonical), 1)
        self.assertTrue(all(r['outcome']['status'] == 'published' for r in results))

    def test_fault_scenarios(self):
        with serve() as base:
            retransmit = read_incremental(base + '/incremental?scenario=retransmit')
            conflict = read_incremental(base + '/incremental?scenario=conflict')
            unknown = read_incremental(base + '/incremental?scenario=unknown')
            accepted = read_incremental(base + '/incremental?scenario=errors',
                                        accept_errors=[['company', 'certificates']])
            withheld = read_incremental(base + '/incremental?scenario=errors')
            truncated = read_incremental(base + '/truncated')
        self.assertEqual(retransmit['outcome']['duplicates'], 1)
        self.assertEqual(len(conflict['outcome']['conflicts']), 1)
        self.assertEqual(unknown['outcome']['failure']['reason'], '未登记的 pending id')
        self.assertEqual(accepted['outcome']['status'], 'published')
        self.assertEqual(accepted['outcome']['errors'][0]['path'], ['company', 'certificates'])
        self.assertEqual(withheld['outcome']['status'], 'withheld')
        self.assertEqual(truncated['outcome']['failure']['reason'], '连接中断')

    def test_cancel_stops_reading(self):
        with serve() as base:
            result = read_incremental(base + '/incremental', chunk_size=32, cancel_after=2)
        self.assertEqual(result['outcome']['status'], 'cancelled')
        self.assertLess(len(result['raw']), len(response_bytes()))
        self.assertIsNone(result['outcome']['data'])
        partial = result['snapshot']
        self.assertIsNotNone(partial['data'])  # 首片已处理，半成品可供诊断
        self.assertEqual(partial['branches']['people'], '等待中')

    def test_incremental_entry_rejects_plain_json(self):
        with serve() as base:
            result = read_incremental(base + '/plain')
        self.assertEqual(result['outcome']['failure']['reason'], '不是 multipart/mixed 响应')


if __name__ == '__main__':
    unittest.main()
