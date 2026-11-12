# 园区目录接口接收器

读取本机普通 JSON 响应，并把接收到的原始响应字节与头信息保存。

## 运行

在 Python 3.11 或更新版本的 Linux 环境使用。原始内容保存在 `output/raw.sqlite3`，删除 `output/` 可回到干净状态。全部示例使用本地合成数据，不包含账号凭据或真实企业资料。

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q .
python3 app.py demo
```

`app.py demo` 直接执行当前读取路径并输出结构化摘要，不重放历史日志；正常执行不到 30 秒。`--output` 可以指定另一份原文库。

## 模块与数据

source.py 提供 HTTP 响应，fixtures/protocol.json 描述支持的服务方协议。

本机 `/incremental` 接口可选 `?scenario=retransmit`、`conflict`、`unknown` 或 `errors`，分别注入重复补丁、矛盾内容、未登记分支和字段错误。`/truncated` 返回缺尾的内容；传输块保持七字节，词法边界不会与网络读取边界对齐。

`raw_store.py` 将原始字节按 SHA-256 保存，用来源和字节摘要区分观察。它只保存原文，不表达业务结果是否完整。同一内容重复观察保留一份对象并更新观察时间，SQLite 事务保证对象和观察同时提交。所有解析异常保持非零退出码，输入原件不改写。

## 环境

`requirements.txt` 登记精确依赖。环境准备阶段安装依赖，运行、构建及测试不执行下载。正式运行可以使用依赖准备后记录的镜像 ID；镜像和验证回执保存在项目外，不影响原始内容。
