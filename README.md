# Bookmark Store baseline

这是本次提案研究中新建的 Python 标准库资产。它不是现有生产系统，
也不是 benchmark 官方源码或客户已验收成果。本目录提供可实际运行的 latest-only
存储基线；命名快照与部分分段压缩是拟议新增功能，当前没有实现。

## 运行

在本目录使用 Python 3.9 或更新版本执行：

```sh
python -m bookmark_store.cli --root /tmp/bookmark-demo
```

向标准输入逐行发送 JSON，程序对每行输出一个 JSON 并持续运行到 EOF：

```json
{"op":"write","items":[{"key":"order:1","value":"paid"},{"key":"old","value":null}]}
{"op":"get","key":"order:1"}
{"op":"scan"}
{"op":"compact"}
{"op":"info"}
```

成功响应分别包含 `seq`、`value`、`items`，compact 仅返回 `{"ok":true}`，
info 返回当前 `seq`、活跃段文件名 `segments` 和空的 `snapshots`。
write 必须是非空批次，每个键是非空字符串且批内唯一，value 为字符串或 null。
整批共用一个递增序号，先验证全部输入再修改文件。null 表示删除，scan 不返回删除键。
不存在的 get 返回 null。compact 在保留最新逻辑值的同时合并所有活跃段，保留最新删除记录。

输入错误返回 `INVALID_REQUEST`，未实现操作返回 `UNSUPPORTED_OPERATION`。
pin、unpin、get/scan 的 snapshot 参数，以及 compact 的 segments 参数均明确拒绝，
不会静默按 latest/full compact 执行。未知字段也会拒绝。

## 文件与持久化边界

- `bookmark_store/store.py`：Store、不可变分段、manifest 校验和原子发布。
- `bookmark_store/cli.py`：逐行 JSON 命令适配。
- `bookmark_store/__init__.py`：导出 Store 和 StoreError。
- `tests/test_baseline.py`：批次原子验证、删除、重启、全量压缩与损坏拒绝等基础测试。

数据库目录包含 `manifest.json` 和 `seg-<32位唯一标识>.jsonl`。每次写入形成一个新段；
段记录按 key 升序、seq 降序排列。读取按 seq 取最新值，不依赖 manifest 中的文件顺序。
manifest 的 `format` 为 1，记录提交序号、活跃段和空的 snapshots 映射。

新段和 manifest 先写入临时文件并 fsync，再通过 os.replace 发布。Linux 上会对目录
执行 fsync；旧活跃文件只在新 manifest 发布后删除。重启先完整验证 manifest 及其
所有活跃段，再清理未引用文件；损坏或缺失活跃文件返回 `CORRUPT_STORE`，不会重置空库。
没有 manifest 而已有段文件也拒绝打开。Windows 可以做功能开发，但不具备本实现
在 Linux 上执行的目录 fsync，因此不能用 Windows 功能测试宣称 Linux 持久化验证通过。

仅支持一个进程/实例在某一时刻使用目录，实例和进程应顺序运行。没有多写者并发控制。
IO_ERROR 表示系统文件操作失败；应停止使用该实例并排查，再重新打开磁盘状态。
此项目没有硬件断电实测记录；进程终止测试不能代替硬件掉电证明。

## 崩溃测试观察点

设置 `BOOKMARK_PAUSE_AT` 为 `before_publish`、`after_publish` 或 `after_cleanup`，
正常变更执行到相应边界会在 stderr 输出 `{"phase":"相应名称"}` 并暂停。
测试程序可以等待标记再终止自己的子进程。初始化空库不暂停。正常使用不要设置该变量。
这些是公开的发布阶段观察点，不含隐藏测试编号、答案或业务规则。

运行基础测试：

```sh
python -m unittest discover -s tests -v
```
