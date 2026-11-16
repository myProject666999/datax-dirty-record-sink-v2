# DataX 脏数据回放（dirty record replay）使用指南

## 1. 适用场景

开启[脏数据落盘](dirty-record-sink.md)的作业，在修复了导致脏数据的问题后（例如修正了
目标表字段长度、约束、类型转换等），可以只把落盘的脏数据重新送入 writer，而不必重跑
整个同步 job。

回放入口是独立的主类 `com.alibaba.datax.core.replay.DirtyRecordReplayEngine`，
通过 `bin/datax-replay.py` 启动；它只加载原作业的 **writer 插件**，不启动 reader、
不建 Channel，按顺序把 sink 目录里的 JSONL 记录还原成 Record 交给 writer。

## 2. 命令与参数

```bash
python $DATAX_HOME/bin/datax-replay.py \
    --sink /data/dirty-sinks/orderjob-20261116 \
    --job  /etc/datax/original-job.json \
    [--force] \
    [--allow-manifest-missing] \
    [--re-dirty-sink /data/dirty-replay/attempt2] \
    [-j "-Xmx2g"]
```

| 参数 | 必填 | 说明 |
|---|---|---|
| --sink | 是 | 脏数据落盘目录（含 manifest.json 与 *.jsonl） |
| --job | 是 | 原始同步作业的 json，回放只取 `job.content[0].writer` 的 name 与 parameter；允许修改 writer 参数（这正是「修复后回放」的用法），但 writer 插件名必须仍可加载 |
| --force | 否 | 已存在 `_REPLAYED` 标记时仍强制重复回放 |
| --allow-manifest-missing | 否 | manifest 缺失时允许降级直接按 jsonl 回放（跳过 sha256/记录数校验，会大声告警） |
| --re-dirty-sink | 否 | 回放中再次变脏记录的输出根目录，默认 `<sink>/replay-redirty-<时间戳>` |
| -j | 否 | 透传 JVM 参数 |

## 3. 回放前的完整性校验

回放引擎在写入前依次检查：

1. **重复回放防护**：sink 目录存在 `_REPLAYED` 标记且未加 `--force` 时，直接报错
   （Framework-24），避免同一批数据重复写入。
2. **半文件检测**：存在 `.jsonl.writing` 残留（进程被 kill -9 等）时报错并列出文件，
   需人工检查处理后再回放。
3. **manifest 门禁**：无 manifest 时必须显式 `--allow-manifest-missing` 才继续。
4. **清单对账**：manifest 与磁盘上的 jsonl 文件集合必须一致，否则报错列出差异。
5. **sha256 校验**：逐文件比对大小与 sha256，任何不一致在回放开始前失败（Framework-25）。

## 4. 损坏文件的报错方式

JSONL 采用逐行严格解析。任何空行、非法 JSON、缺失必填字段（version/columns/pluginType）、
未知列类型、base64/epoch 数据无法解析等问题都会中断回放并抛出 Framework-23，
错误信息包含精确定位：

```
脏数据文件损坏: file=/data/dirty-sinks/.../dirty-j1-tg0-t0-a1-writer.000002.jsonl,
line=137, byteOffset=48213, reason=第2列(DATE)的rawData无法解析: ...
```

被截断的末行同样报错，绝不会静默跳过。修复文件后重新执行即可（注意重复回放标记语义）。

## 5. 回放中再次产生脏数据

回放时 writer 仍可能拒绝部分记录。这些「再脏数据」会通过同一个落盘实现
（FileSinkPluginCollector）写入独立的 re-dirty 目录，不会丢失：

- 默认目录：`<原sink>/replay-redirty-<时间戳>/<原sink名>-replay/`
- 每行额外带 `replayOf`（原 sink 名）与 `replayAttempt` 字段，pluginType 固定为 WRITER
- 同样有独立 manifest、sha256、滚动与总量控制
- 该目录可以再次作为 `--sink` 输入进行下一轮回放

## 6. 列类型还原规则

| 落盘类型 | 还原为 |
|---|---|
| LONG / INT | LongColumn（DataX 中 INT 也由 LongColumn 承载） |
| DOUBLE | DoubleColumn |
| BOOL | BoolColumn |
| STRING / BAD | StringColumn（无 BadColumn 实现，BAD 保留负载文本） |
| DATE | DateColumn，按 epochMillis 还原 |
| BYTES | BytesColumn，按 base64 解码 |
| NULL | rawData=null 的空列 |
| 脱敏列（masked=true） | 按落盘的 STRING 值（摘要/替换值）回放，**原值不可恢复** |

`meta` 原样还原到 Record 上。

## 7. 典型流程

```bash
# 1) 原作业开启落盘运行，因 errorLimit 失败，留下完整 sink 目录
python bin/datax.py original-job.json

# 2) 修复目标表/作业配置（修改 original-job.json 的 writer parameter）

# 3) 只回放脏数据
python bin/datax-replay.py \
    --sink /data/dirty-sinks/sink-job0-20261116 \
    --job  /etc/datax/original-job-fixed.json

# 4) 若回放仍有脏数据，对 re-dirty 目录继续回放
python bin/datax-replay.py \
    --sink /data/dirty-sinks/sink-job0-20261116/replay-redirty-20261117/sink-job0-20261116-replay \
    --job  /etc/datax/original-job-fixed2.json
```

成功回放后 sink 目录会写入 `_REPLAYED` 标记；重复执行同一回放需要 `--force`。
