# 园区目录接口接收器

读取本机普通 JSON 响应与 GraphQL 增量分片响应，把接收到的原始响应字节与头信息保存，并按 fixtures/protocol.json 约定的增量协议子集裁决是否发布企业档案。

## 运行

在 Python 3.11 或更新版本的 Linux 环境使用。原始内容保存在 `output/raw.sqlite3`，删除 `output/` 可回到干净状态。全部示例使用本地合成数据，不包含账号凭据或真实企业资料。

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q .
python3 app.py demo
```

`app.py demo` 直接执行当前读取路径并输出结构化摘要，不重放历史日志；正常执行不到 30 秒。`--output` 可以指定另一份原文库。

## 接收通道

- `client.read_plain`：普通 JSON 入口，只接受 `application/json`，行为不变。
- `client.read_incremental`：`multipart/mixed` 增量入口，按块流式读取，分片一完整就解析，不把全部字节攒到末尾再处理。boundary 允许带引号；UTF-8 字符与分隔符可以横跨网络块。

`incremental.py` 的 `IncrementalCollector` 是唯一的正式接收路径：不同块大小、乱序分支、错误分支与中途断开都走同一状态机。语义要点：

- 首片携带初始 `data` 与 `pending` 登记，后续分片携带 `incremental` 补丁与 `completed` 关闭通知，`hasNext=false` 终止整个响应；终止时仍有未关闭分支即为不完整。
- 补丁由 `id` 与 `path` 定位；流式列表按 `subPath` 下标落位，不当作追加顺序。
- 完全相同的重传补丁被识别并计数跳过；同一位置的矛盾内容留下冲突记录，不静默覆盖。
- 错误路径上的 `null`（错误置空）与尚未到达的数据分别标记，互不混淆。

## 发布门槛与失败状态

下游用 `accept_errors` 按字段路径声明可接受的错误。只有订阅完整（`hasNext=false` 且分支全部关闭）且 `required_paths` 满足时才发布一份结果（`published`）；接收完整但必需分支未满足为 `withheld`；断流、缺少结束标志、未登记 pending id、路径越界、超过尺寸限制等都是可解释的 `failed` 原因。取消采集（`cancel`）会终止来源读取，状态为 `cancelled`；半成品只能经 `snapshot()` 供诊断，普通成功查询（`outcome()`）读不到。

## 模块与数据

source.py 提供 HTTP 响应，fixtures/protocol.json 描述支持的服务方协议。

本机 `/incremental` 接口可选 `?scenario=retransmit`、`conflict`、`unknown` 或 `errors`，分别注入重复补丁、矛盾内容、未登记分支和字段错误。`/truncated` 返回缺尾的内容；传输块保持七字节，词法边界不会与网络读取边界对齐。

`raw_store.py` 将原始字节按 SHA-256 保存，用来源和字节摘要区分观察。它只保存原文，不表达业务结果是否完整。同一内容重复观察保留一份对象并更新观察时间，SQLite 事务保证对象和观察同时提交。所有解析异常保持非零退出码，输入原件不改写。

## 环境

`requirements.txt` 登记精确依赖。环境准备阶段安装依赖，运行、构建及测试不执行下载。正式运行可以使用依赖准备后记录的镜像 ID；镜像和验证回执保存在项目外，不影响原始内容。
