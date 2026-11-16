# datax-example-dirtyrecord

脏数据落盘与回放能力的端到端测试模块。

## 组成

- `dirtydemoreader`：内存型测试 reader，可按配置主动上报 reader 侧脏数据、
  设置 Record meta（来源信息）、发送超限记录。
- `dirtydemowriter`：内存型测试 writer，可按列值把记录上报为 writer 侧脏数据，
  回放时支持「再脏数据」模式，并可把成功写入的记录输出到文件用于断言。
- 两个插件的 `plugin.json` 位于 `src/main/resources/plugin` 与 `plugin2`，
  会被 `ExampleConfigParser` 的 classpath 扫描发现，无需安装版 DATAX_HOME。

## 覆盖的场景

| 测试类 | 覆盖点 |
|---|---|
| ReaderAndWriterSideSinkE2ETest | reader/writer 两侧脏数据落盘、字段完整、meta 来源信息 |
| ConcurrentSinkE2ETest | 5 channel 多 task 并发、文件滚动、manifest 对账 |
| RollingAndCapE2ETest | 文件滚动、STOP_RECORD 与 FAIL_JOB 两种总量策略 |
| MaskingE2ETest | 按列名 sha256 与按下标 replace 脱敏、原文不落盘 |
| ErrorLimitFlushE2ETest | errorLimit 失败时 _FAILED + manifest 完整、无半文件 |
| DefaultOffE2ETest | 默认关闭时无 sink 目录、行为不变 |
| ReplayE2ETest | 正常回放、损坏定位、重复回放拦截/force、再脏数据再落盘 |

## 离线运行

```bash
mvn -o test -pl datax-example/datax-example-dirtyrecord -Dassembly.skipAssembly=true
```

依赖的 datax-common / datax-core / datax-example-core 需先安装到本地仓库：

```bash
mvn -o install -pl common,core -DskipTests -Dassembly.skipAssembly=true
mvn -o install -pl datax-example/datax-example-core -DskipTests -Dassembly.skipAssembly=true
```
