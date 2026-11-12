import argparse
import json
from datetime import datetime, timezone

from client import read_incremental, read_plain
from raw_store import RawStore
from source import serve


def summarize(result, with_snapshot=False):
    outcome = result['outcome']
    view = {'发布状态': outcome['status']}
    if outcome['data'] is not None:
        view['最终数据'] = outcome['data']
    if outcome['errors']:
        view['错误路径'] = [error['path'] for error in outcome['errors']]
    if outcome['conflicts']:
        view['冲突'] = outcome['conflicts']
    if outcome['duplicates']:
        view['重复补丁'] = outcome['duplicates']
    if outcome['unmet']:
        view['未满足必需分支'] = outcome['unmet']
    if outcome['failure']:
        view['失败原因'] = outcome['failure']
    if with_snapshot:
        view['诊断快照'] = result['snapshot']
    return view


def demo(store):
    observed_at = datetime.now(timezone.utc).isoformat()
    digests = []
    report = {}
    with serve() as base:
        raw, parsed = read_plain(base + '/plain')
        digests.append(store.save('directory:plain', raw, {'content_type': 'application/json'}, observed_at))
        report['普通JSON入口'] = {'data': parsed['data'], '说明': '普通 JSON 请求入口保留不变'}

        channel = {}
        canonical = set()
        for size in (1, 7, 4096):
            result = read_incremental(base + '/incremental', chunk_size=size)
            digests.append(store.save(f'directory:incremental:normal:block{size}', result['raw'],
                                      {'content_type': 'multipart/mixed', '块大小': size}, observed_at))
            channel[f'正常-块大小{size}'] = summarize(result)
            canonical.add(json.dumps(result['outcome']['data'], ensure_ascii=False, sort_keys=True))
        channel['块大小结果一致'] = len(canonical) == 1

        for label, scenario in (('乱序下标-重复补丁', 'retransmit'),
                                ('乱序下标-矛盾内容', 'conflict'),
                                ('未登记分支', 'unknown')):
            result = read_incremental(base + f'/incremental?scenario={scenario}')
            digests.append(store.save(f'directory:incremental:{scenario}', result['raw'],
                                      {'content_type': 'multipart/mixed', 'scenario': scenario}, observed_at))
            channel[label] = summarize(result)

        result = read_incremental(base + '/incremental?scenario=errors',
                                  accept_errors=[['company', 'certificates']])
        digests.append(store.save('directory:incremental:errors', result['raw'],
                                  {'content_type': 'multipart/mixed', 'scenario': 'errors'}, observed_at))
        channel['错误分支-声明可接受'] = summarize(result, with_snapshot=True)
        result = read_incremental(base + '/incremental?scenario=errors')
        channel['错误分支-未声明可接受'] = summarize(result, with_snapshot=True)

        result = read_incremental(base + '/truncated')
        digests.append(store.save('directory:incremental:truncated', result['raw'],
                                  {'content_type': 'multipart/mixed', 'scenario': 'truncated'}, observed_at))
        channel['中途断开'] = summarize(result)

        result = read_incremental(base + '/incremental', chunk_size=64, cancel_after=2)
        digests.append(store.save('directory:incremental:cancelled', result['raw'],
                                  {'content_type': 'multipart/mixed', 'scenario': 'cancelled'}, observed_at))
        channel['取消采集'] = summarize(result, with_snapshot=True)
        report['增量接收通道'] = channel
    report['原文摘要'] = digests
    return report


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
