"""GraphQL 增量分片响应的流式接收。

按照 fixtures/protocol.json 约定的协议子集处理 multipart/mixed 响应：
首片携带初始 data 与 pending 登记，后续分片携带 incremental 补丁与
completed 关闭通知，hasNext=false 终止整个响应。接收按网络块流式进行，
分片一完整就立即解析，不把全部字节攒到末尾再处理。
"""

import copy
import json


class ReceiveFailure(Exception):
    """可解释的接收失败：reason 说明类别，detail 给出上下文。"""

    def __init__(self, reason, detail=''):
        self.reason = reason
        self.detail = detail
        super().__init__(f'{reason}（{detail}）' if detail else reason)


MISSING = object()


def extract_boundary(content_type):
    """从 Content-Type 取出 boundary；boundary 可以带引号。"""
    if not content_type:
        raise ReceiveFailure('缺少 Content-Type')
    segments = content_type.split(';')
    if segments[0].strip().lower() != 'multipart/mixed':
        raise ReceiveFailure('不是 multipart/mixed 响应', segments[0].strip())
    for parameter in segments[1:]:
        key, _, value = parameter.partition('=')
        if key.strip().lower() != 'boundary':
            continue
        value = value.strip()
        if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        try:
            return value.encode('ascii')
        except UnicodeEncodeError:
            break
    raise ReceiveFailure('boundary 缺失或与协议不符', content_type)


class MultipartSplitter:
    """把字节流按 boundary 切成分片。

    分隔符与 UTF-8 字符都可能横跨网络块：匹配在字节层面进行，
    不完整的分隔符尾部会保留到下一个块到达，单个分片超过尺寸
    上限时立即失败，而不是等整个响应读完。
    """

    def __init__(self, boundary, max_part_bytes, on_part, on_final):
        self.delimiter = b'--' + boundary
        self.max_part_bytes = max_part_bytes
        self.on_part = on_part
        self.on_final = on_final
        self.buffer = bytearray()
        self.part = bytearray()
        self.started = False
        self.closed = False

    def feed(self, chunk):
        if self.closed:
            return  # 结束边界之后是尾声，丢弃
        self.buffer += chunk
        self._drain()

    def finish(self):
        if not self.closed:
            raise ReceiveFailure('连接中断', '响应在结束边界之前断开')

    def _drain(self):
        while True:
            index = self.buffer.find(self.delimiter)
            if index < 0:
                # 分隔符可能横跨块，保留尾部等待后续字节
                keep = len(self.delimiter) - 1
                if len(self.buffer) > keep:
                    self._take(len(self.buffer) - keep)
                return
            if len(self.buffer) < index + len(self.delimiter) + 2:
                # 等待分隔符后的 \r\n 或 --
                if index:
                    self._take(index)
                return
            self._take(index)
            tail = bytes(self.buffer[len(self.delimiter):len(self.delimiter) + 2])
            del self.buffer[:len(self.delimiter)]
            if tail == b'\r\n':
                del self.buffer[:2]
                self._emit()
                self.started = True
                continue
            if tail == b'--':
                self._emit()
                self.closed = True
                self.on_final()
                return
            raise ReceiveFailure('multipart 结构损坏', f'分隔符后出现意外字节 {tail!r}')

    def _take(self, count):
        if self.started:
            self.part += self.buffer[:count]
            if len(self.part) > self.max_part_bytes:
                raise ReceiveFailure('分片超过尺寸限制', f'上限 {self.max_part_bytes} 字节')
        del self.buffer[:count]

    def _emit(self):
        if not self.started:
            return  # 第一个分隔符之前是前言
        raw = bytes(self.part)
        self.part.clear()
        if len(raw) > self.max_part_bytes:
            raise ReceiveFailure('分片超过尺寸限制', f'上限 {self.max_part_bytes} 字节')
        if raw.startswith(b'\r\n'):
            raw = raw[2:]
        if raw.endswith(b'\r\n'):
            raw = raw[:-2]
        _, separator, body = raw.partition(b'\r\n\r\n')
        if not separator:
            raise ReceiveFailure('multipart 结构损坏', '分片缺少头部分隔')
        self.on_part(body)


class IncrementalCollector:
    """增量协议的流式状态机：登记分支、应用补丁、裁决发布。

    任何时刻都可以调用 snapshot() 取诊断快照（半成品可见）；只有
    订阅完整且必需分支满足条件时 status() 才是 'published'，
    outcome() 才携带数据，普通成功查询读不到半份结果。
    """

    def __init__(self, protocol, accept_errors=()):
        self.max_body_bytes = protocol['max_body_bytes']
        self.max_part_bytes = protocol['max_part_bytes']
        self.required_paths = [tuple(path) for path in protocol['required_paths']]
        self.accept_errors = {tuple(path) for path in accept_errors}
        self.data = None
        self.branches = {}
        self.errors = []
        self.conflicts = []
        self.duplicates = 0
        self.parts_seen = 0
        self.received_bytes = 0
        self.has_next = True
        self.transport_complete = False
        self.failure = None
        self.cancelled = False
        self._seen_payloads = {}
        self._slots = {}
        self._error_paths = set()
        self._splitter = None
        self._first_part = True

    # 传输层 -------------------------------------------------------------

    def begin(self, boundary):
        """boundary 来自响应头，可以是 str 或 bytes。"""
        if isinstance(boundary, str):
            boundary = boundary.encode('ascii')
        self._splitter = MultipartSplitter(
            boundary, self.max_part_bytes, self._on_part, self._on_final)

    def feed(self, chunk):
        """送入一个网络块；分片完整即解析，失败立即抛出。"""
        if self.cancelled:
            raise ReceiveFailure('已取消', '采集被取消，停止读取来源')
        if self.failure is not None:
            raise self.failure
        if self._splitter is None:
            raise ReceiveFailure('接收器未初始化', '先调用 begin')
        self.received_bytes += len(chunk)
        if self.received_bytes > self.max_body_bytes:
            self.failure = ReceiveFailure('响应体超过尺寸限制', f'上限 {self.max_body_bytes} 字节')
            raise self.failure
        try:
            self._splitter.feed(chunk)
        except ReceiveFailure as exc:
            self.failure = exc
            raise

    def cancel(self):
        """取消采集：调用方应停止读取来源，半成品仅供诊断。"""
        self.cancelled = True

    def finish(self):
        """来源读到末尾后的收尾裁决。"""
        if self.cancelled or self.failure is not None:
            return
        try:
            self._splitter.finish()
        except ReceiveFailure as exc:
            self.failure = exc
            return
        if self.has_next:
            self.failure = ReceiveFailure('缺少结束标志', '响应终止前未收到 hasNext=false')
            return
        waiting = [pid for pid, branch in self.branches.items() if not branch['done']]
        if waiting:
            self.failure = ReceiveFailure('分支未关闭', '、'.join(waiting))

    # 协议层 -------------------------------------------------------------

    def _on_final(self):
        self.transport_complete = True

    def _on_part(self, body):
        try:
            part = json.loads(body)
        except ValueError as exc:
            raise ReceiveFailure('分片不是合法 JSON', str(exc))
        if not isinstance(part, dict):
            raise ReceiveFailure('分片结构不符', '分片必须是对象')
        if not self.has_next:
            raise ReceiveFailure('multipart 结构损坏', 'hasNext=false 之后仍收到分片')
        self.parts_seen += 1
        if self._first_part:
            self._first_part = False
            self._handle_initial(part)
        else:
            self._handle_followup(part)

    def _handle_initial(self, part):
        data = part.get('data')
        if not isinstance(data, dict):
            raise ReceiveFailure('首片缺少 data')
        self.data = copy.deepcopy(data)
        for entry in part.get('pending') or []:
            pid = entry.get('id')
            path = entry.get('path')
            if not pid or not isinstance(path, list):
                raise ReceiveFailure('pending 登记不完整', json.dumps(entry, ensure_ascii=False))
            self._resolve(tuple(path))  # 登记的路径必须落在初始数据内
            self.branches[pid] = {'path': tuple(path), 'done': False}
        self._record_errors(part.get('errors'))
        self._update_has_next(part)

    def _handle_followup(self, part):
        for patch in part.get('incremental') or []:
            self._apply_patch(patch)
        for entry in part.get('completed') or []:
            self._complete_branch(entry)
        self._record_errors(part.get('errors'))
        self._update_has_next(part)

    def _update_has_next(self, part):
        self.has_next = bool(part.get('hasNext', True))

    def _apply_patch(self, patch):
        pid = patch.get('id')
        branch = self.branches.get(pid)
        if branch is None:
            raise ReceiveFailure('未登记的 pending id', str(pid))
        canonical = json.dumps(patch, ensure_ascii=False, sort_keys=True)
        seen = self._seen_payloads.setdefault(pid, set())
        if canonical in seen:
            self.duplicates += 1  # 完全相同的重传：识别并跳过
            return
        seen.add(canonical)
        self._record_errors(patch.get('errors'))
        data = patch.get('data')
        if isinstance(data, dict):
            target = self._resolve(branch['path'])
            if not isinstance(target, dict):
                raise ReceiveFailure('路径越界', f'id={pid} 的 data 补丁目标不是对象')
            self._merge(target, data, branch['path'])
        items = patch.get('items')
        if items is not None:
            if not isinstance(items, list):
                raise ReceiveFailure('分片结构不符', 'items 必须是列表')
            self._insert_items(pid, branch, tuple(patch.get('subPath') or []), items)

    def _complete_branch(self, entry):
        pid = entry.get('id')
        branch = self.branches.get(pid)
        if branch is None:
            raise ReceiveFailure('未登记的 pending id', str(pid))
        self._record_errors(entry.get('errors'))
        branch['done'] = True

    def _record_errors(self, errors):
        for error in errors or []:
            path = tuple(error.get('path') or ())
            self.errors.append({'message': error.get('message', ''), 'path': list(path)})
            self._error_paths.add(path)

    # 数据层 -------------------------------------------------------------

    def _resolve(self, path):
        node = self.data
        for key in path:
            if isinstance(node, dict) and key in node:
                node = node[key]
            elif (isinstance(node, list) and isinstance(key, int)
                  and not isinstance(key, bool) and 0 <= key < len(node)):
                node = node[key]
            else:
                raise ReceiveFailure('路径越界', f'无法到达 {list(path)}')
        return node

    def _merge(self, target, patch, path):
        for key, value in patch.items():
            if key not in target:
                target[key] = copy.deepcopy(value)
                continue
            old = target[key]
            if isinstance(old, dict) and isinstance(value, dict):
                self._merge(old, value, path + (key,))
            elif old != value:
                # 同一位置内容矛盾：保留先到的内容并留下冲突，不静默覆盖
                self.conflicts.append({'path': list(path + (key,)), '保留': old, '收到': value})

    def _insert_items(self, pid, branch, sub_path, items):
        if (not sub_path or not isinstance(sub_path[-1], int)
                or isinstance(sub_path[-1], bool) or sub_path[-1] < 0):
            raise ReceiveFailure('路径越界', f'id={pid} 的 subPath 缺少合法下标')
        list_path = branch['path'] + sub_path[:-1]
        self._resolve(list_path)  # 列表容器必须存在
        slots = self._slots.setdefault(list_path, {})
        index = sub_path[-1]
        for offset, item in enumerate(items):
            position = index + offset
            if position in slots:
                if slots[position] != item:
                    self.conflicts.append(
                        {'path': list(list_path) + [position], '保留': slots[position], '收到': item})
                continue
            slots[position] = copy.deepcopy(item)

    # 裁决层 -------------------------------------------------------------

    def materialized_data(self):
        """把流式列表按 subPath 下标落位后的数据视图（乱序不当作追加）。"""
        if self.data is None:
            return None
        return self._materialize(self.data, ())

    def _materialize(self, node, path):
        if path in self._slots:
            slots = self._slots[path]
            base = node if isinstance(node, list) else []
            size = max([len(base)] + [index + 1 for index in slots])
            view = [None] * size
            for index, value in enumerate(base):
                view[index] = self._materialize(value, path + (index,))
            for index, value in slots.items():
                view[index] = self._materialize(value, path + (index,))
            return view
        if isinstance(node, dict):
            return {key: self._materialize(value, path + (key,)) for key, value in node.items()}
        if isinstance(node, list):
            return [self._materialize(value, path + (index,)) for index, value in enumerate(node)]
        return node

    def path_state(self, path):
        """区分已到达、错误置空与尚未到达，三者不混为一谈。"""
        path = tuple(path)
        value = self._lookup(path)
        if value is MISSING:
            return '尚未到达'
        if value is None and any(self._covers(error_path, path) for error_path in self._error_paths):
            return '错误置空'
        return '已到达'

    def unmet_required(self):
        unmet = []
        for path in self.required_paths:
            state = self.path_state(path)
            if state == '已到达':
                continue
            if state == '错误置空':
                covering = [error for error in self.errors
                            if self._covers(tuple(error['path']), path)]
                if covering and all(tuple(error['path']) in self.accept_errors for error in covering):
                    continue  # 下游已声明该字段的错误可接受
                unmet.append({'path': list(path), '原因': '错误未声明可接受'})
            else:
                unmet.append({'path': list(path), '原因': state})
        return unmet

    def status(self):
        if self.cancelled:
            return 'cancelled'
        if self.failure is not None:
            return 'failed'
        if not self.transport_complete or self.has_next:
            return 'receiving'
        if self.unmet_required():
            return 'withheld'
        return 'published'

    def outcome(self):
        """普通查询视角：只有 published 才携带数据。"""
        status = self.status()
        return {
            'status': status,
            'data': self.materialized_data() if status == 'published' else None,
            'errors': copy.deepcopy(self.errors),
            'conflicts': copy.deepcopy(self.conflicts),
            'duplicates': self.duplicates,
            'unmet': self.unmet_required() if status == 'withheld' else [],
            'failure': None if self.failure is None else {
                'reason': self.failure.reason, 'detail': self.failure.detail},
        }

    def snapshot(self):
        """诊断视角：任何时刻可用，半成品也能看到。"""
        return {
            'status': self.status(),
            'received_bytes': self.received_bytes,
            'parts': self.parts_seen,
            'branches': {pid: ('已关闭' if branch['done'] else '等待中')
                         for pid, branch in self.branches.items()},
            'required': {'/'.join(map(str, path)): self.path_state(path)
                         for path in self.required_paths},
            'data': self.materialized_data(),
            'errors': copy.deepcopy(self.errors),
            'conflicts': copy.deepcopy(self.conflicts),
            'duplicates': self.duplicates,
        }

    def _lookup(self, path):
        node = self.materialized_data()
        for key in path:
            if isinstance(node, dict) and key in node:
                node = node[key]
            elif isinstance(node, list) and isinstance(key, int) and 0 <= key < len(node):
                node = node[key]
            else:
                return MISSING
        return node

    @staticmethod
    def _covers(error_path, path):
        return path[:len(error_path)] == error_path
