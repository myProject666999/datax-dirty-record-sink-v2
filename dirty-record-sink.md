# DataX 脏数据落盘（dirty record sink）使用指南

## 1. 功能概述

DataX 同步过程中，少量记录可能因类型不匹配、长度超限、约束冲突等被判定为脏数据。
默认情况下脏数据只打印到日志（受 `maxDirtyNumber` 与日志滚动限制，可能丢失）。

开启脏数据落盘后，每条脏数据会以结构化 JSONL 格式写入指定目录，支持：

- 记录原始各列内容、出错原因、reader/writer 来源、所属 task、来源定位信息（meta）
- 按 task 分文件，多 task 并发互不冲突
- 单文件大小限制与自动滚动
- 落盘总量上限，超限策略可选「停止记录」或「让 job 失败」
- 按列名/列下标配置不可逆脱敏（SHA-256 哈希或固定值替换）
- 与 `errorLimit` 正确配合：job 因脏数据超限失败时，已落盘内容保证完整
- 修复问题后可用[回放工具](dirty-record-replay.md)只重放脏数据，无需重跑整个 job

**默认关闭**：不配置 `job.setting.dirtyRecord`（或 `enable=false`）时行为与历史版本完全一致。

## 2. 配置方式

在作业配置的 `job.setting` 下增加 `dirtyRecord`：

```json
{
  "job": {
    "setting": {
      "speed": { "channel": 3 },
      "errorLimit": { "record": 0 },
      "dirtyRecord": {
        "enable": true,
        "path": "/data/dirty-sinks",
        "sinkName": "orderjob-20261116",
        "fileMaxSize": "64MB",
        "totalSizeCap": "1GB",
        "overCapPolicy": "STOP_RECORD",
        "flushEveryRecord": true,
        "mask": {
          "unknownColumn": "ERROR",
          "rules": [
            { "columnName": "id_card", "strategy": "sha256", "salt": "可选静态盐" },
            { "columnName": "phone",   "strategy": "replace", "replacement": "***" },
            { "columnIndex": 7,        "strategy": "sha256" }
          ]
        }
      }
    },
    "content": [ ... ]
  }
}
```

### 配置项说明

| 键 | 默认值 | 说明 |
|---|---|---|
| enable | false | 总开关。关闭时零开销，行为与历史版本一致 |
| path | enable=true 时必填 | 落盘根目录，不存在会自动创建；创建失败 job 快速报错 |
| sinkName | `sink-job<jobId>-<时间戳>` | 本次落盘的子目录名，不同 job 天然隔离 |
| fileMaxSize | 64MB | 单文件滚动阈值，支持 `64MB`/`1GB`/纯字节数 |
| totalSizeCap | 1GB | 本 job 所有文件的总量上限 |
| overCapPolicy | STOP_RECORD | `STOP_RECORD`：达到上限后脏数据只计数/打日志不再落盘，job 继续；`FAIL_JOB`：达到上限立即抛错使 job 失败 |
| flushEveryRecord | true | 每行刷盘，保证异常退出时已处理行完整 |
| mask.rules | 无 | 脱敏规则列表，见下节 |
| mask.unknownColumn | ERROR | `ERROR`：规则里的列名无法定位时 job 启动失败；`IGNORE`：告警跳过该规则 |

## 3. 脱敏配置

每条规则至少指定 `columnName` 或 `columnIndex` 之一：

- 按列名：从作业 `job.content` 中 reader/writer 的 `parameter.column` 配置解析列下标，
  兼容字符串列名（`"phone"`）、对象形式（`{"name":"phone"}`、`{"index":1,"name":"phone"}`）。
- 按下标：`columnIndex` 直接指定，作为列名无法解析时的兜底，也适用于没有列名配置的插件。
- 策略：
  - `sha256`：输出原值（可选拼接静态盐）的 SHA-256 十六进制摘要，64 位小写 hex。
    盐只写在作业配置里，不会进入落盘文件，可抵御低基数列的预计算表攻击。
  - `replace`：输出固定字面量（默认 `***`），原值被完全丢弃。
- null 值不脱敏，保留 NULL 语义。
- **不可逆说明**：落盘文件中只存在摘要或替换值，无法还原原始内容；哈希的确定性
  （相同值得相同摘要）仅用于调试关联，不是还原路径。被脱敏的列在回放时写入的是脱敏后的值。

## 4. 落盘目录与文件格式

```
<path>/<sinkName>/
├── manifest.json                 # 清单，job 结束时原子写入
├── _SUCCESS 或 _FAILED           # job 状态标记
├── dirty-j<jobId>-tg<taskGroupId>-t<taskId>-a<attempt>-reader.000001.jsonl
├── dirty-j<jobId>-tg<taskGroupId>-t<taskId>-a<attempt>-writer.000001.jsonl
└── ...
```

- 文件名中的 `attempt` 用于区分 failover 重试（同一 task 重试产生新文件，不会覆盖）。
- 正在写入的文件带 `.jsonl.writing` 后缀，滚动或关闭后原子改名为 `.jsonl`；
  崩溃残留的 `.writing` 文件不会被 manifest 收录，回放时会明确报错。

### JSONL 每行字段

| 字段 | 说明 |
|---|---|
| version | 行格式版本 |
| jobId / taskGroupId / taskId / attempt | 所属任务定位 |
| pluginType | READER 或 WRITER，标识脏数据产生侧 |
| timestamp / sequence | 产生时间戳与该文件内单调序号 |
| errorMessage | 错误提示信息（空则省略） |
| exceptionClass / exceptionMessage | 异常类型与异常消息（无异常则省略） |
| meta | 插件通过 `Record.setMeta` 提供的来源信息（如 filePath），原样保存 |
| columnCount / columns | 原始各列；每列含 index、type、rawData，BYTES 用 base64、DATE 用 epochMillis；脱敏列带 `masked=true` 与 `maskStrategy` |

### manifest.json

记录 jobId、sinkName、jobStatus（SUCCESS/FAILED）、文件总数/记录数/总字节、
是否触顶、丢弃记录数、脱敏列、每个文件的文件名/记录数/字节数/sha256/序号区间。
采用「先写 .tmp 再 rename」的原子方式，不会出现写了一半的 manifest。

## 5. 与 errorLimit 的配合

- 落盘不改变 Communication 中的脏数据计数，`errorLimit.record/percentage` 语义完全不变。
- 当脏数据超过 errorLimit 导致 job 失败时：调度器中断 task 线程 → 所有落盘 channel
  flush 并 finalize → 原子写 manifest（jobStatus=FAILED）→ 写 `_FAILED` 标记。
  已 finalize 的 jsonl 每行完整，可直接用于回放。
- 另有 JVM shutdown hook 兜底：SIGTERM 等非正常退出也会尽量 flush/close。
- 若进程被 `kill -9` 强杀，活动文件保留 `.writing` 后缀，回放工具会按
  「文件名 + 行号 + 字节偏移」精确报告损坏位置，不会静默跳过。

## 6. 落盘 IO 错误与总量超限

- 目录不可写、文件创建/写入失败等 IO 错误默认直接使 job 失败，避免静默丢失脏数据。
- `overCapPolicy=STOP_RECORD` 达到总量后，后续脏数据仍计入 Communication 计数
  （因此 errorLimit 仍可能触发），只是不再写文件；manifest 中 `capReached=true`、
  `droppedRecords` 记录丢弃条数。
- `overCapPolicy=FAIL_JOB` 达到总量立即抛 `Framework-21` 使 job 失败。
