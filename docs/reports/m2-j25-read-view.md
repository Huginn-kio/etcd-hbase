# J25 一致读视图验证报告

## 结果

J25 的公开实现与本地验证已交付。本报告记录的是 Java 21 构建、core 测试和本地 HBase MiniCluster 证据，不代表真实目标集群验收，也不冻结最终历史布局。

## 实现范围

- `ReadViewService` 从强一致控制状态捕获最新或指定历史 revision，加载已选择发布的封存根，注册读取保护后复核 compact 边界和根身份。
- `ReadView` 固定目标 revision、稳定物理基线、发布决定、授权范围摘要和 pin 代次。
- `ReadViewRecordStore` 将物理读取限制在稳定基线，并在点读、Range 页面和 Exists 可见性选择前叠加已选择的私有候选；未选择的候选不会进入结果。
- `ManifestCatalog` 按逻辑 key 范围和盐桶有界遍历候选目录，校验引用、候选内容、revision 和桶，并记录 manifest 读取成本。
- 读取取消或关闭后，pin 等待正在进行的物理 producer/scanner 停止再释放。

实现保持在 core 的内部契约中，没有把 HBase、gRPC 或提交状态类型加入公共 SPI。Range 的 head 与后取 value、点 Exists 和范围 Exists 均通过同一个 `ReadViewRecordStore` 完成。

## 验证

基线为当前重排后 J24 commit `cb103ce9d1bb30dbc7bc95cecf310ca9a7bdeee5`。执行命令：

```sh
ETCD_HBASE_JDK21_HOME=<Temurin JDK 21 home> scripts/verify/j25.sh
```

最终运行结果：

| 范围 | 测试数 | 失败 | 错误 | 跳过 |
|---|---:|---:|---:|---:|
| SPI 单元测试 | 13 | 0 | 0 | 0 |
| core 单元测试 | 100 | 0 | 0 | 0 |
| HBase 单元测试 | 119 | 0 | 0 | 0 |
| J25 HBase MiniCluster | 1 | 0 | 0 | 0 |

Core 回归用例验证：

- Range head 与后取 value、点 Exists 和范围 Exists 使用同一个 `ReadViewRecordStore`。
- pin 注册后的控制状态/根复核，以及取消后等待物理 producer 完成再释放 pin。
- 未选中的同 key 候选不覆盖已选择版本。

HBase MiniCluster 用同一组逻辑操作分别验证 `HISTORY` 与 `NATIVE` 两种物理布局，包括：

- 已选择发布的更新、删除、插入和删除后重建对当前读可见。
- 在删除 revision 读取历史值和墓碑；历史 Range 使用同一 revision 枚举历史键。
- 与已选择候选同 key、同 revision 的未提交候选仍不可见。
- 元数据与选中 value 的 revision/createRevision/version 一致。
- Range 按 key 顺序合并稳定物理基线与已选择候选。

运行脚本生成的机器可读摘要和原始测试日志保存在本地忽略目录 `.artifacts/m2/j25/`，不纳入 PR。

## 证据边界

- MiniCluster 的物理 `HISTORY`/`NATIVE` 访问使用真实 HBase 表；本报告中的控制状态、manifest repository 和 pin registry 是测试替身。因此未验证这些元数据的 HBase 持久化原子性或目标环境身份配置。
- `ReadViewPinRegistry` 是 J26 持久跨副本 pin manager 的接口边界。MiniCluster 使用测试 pin，未证明跨副本 pin 注册与 GC 的竞争安全。
- 两种布局的正确性路径都执行了；没有做足以选定生产布局的物理空间、compaction、p99 或接管成本比较。最终选择等待 J52.1。
- 没有真实目标 HBase 集群、Go client RPC、生产级恢复或多机故障证据；这些分别由后续任务验收。
- 作者按正确性、职责/依赖、可读性、异常与取消、资源所有权/清理、安全边界和有界扫描成本完成自审。独立 agent 复核未执行；维护者评审尚待 PR。
