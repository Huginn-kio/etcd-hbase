# J25 一致读视图验证报告

## 1. 实现范围

- `ReadViewService` 从强一致控制状态捕获最新或指定历史 revision，加载已选择发布的封存根，注册读取保护后复核 compact 边界和根身份。
- `ReadView` 固定目标 revision、稳定物理基线、发布决定、授权范围摘要和 pin 代次。
- `ReadViewRecordStore` 将物理读取限制在稳定基线，并在点读、Range 页面和 Exists 可见性选择前叠加已选择的私有候选；未选择的候选不会进入结果。
- `ManifestCatalog` 按逻辑 key 范围和盐桶有界遍历候选目录，校验引用、候选内容、revision 和桶，并记录 manifest 读取成本。
- 读取取消或关闭后，pin 等待正在进行的物理 producer/scanner 停止再释放。

实现保持在 core 的内部契约中，没有把 HBase、gRPC 或提交状态类型加入公共 SPI。Range 的 head 与后取 value、点 Exists 和范围 Exists 均通过同一个 `ReadViewRecordStore` 完成。

## 2. 历史运行与版本绑定

旧报告中的 J25 本地运行对应源提交 `58d6008e303fac1916c33624b11291a8d69a9d53`，其直接父提交为 `cb103ce9d1bb30dbc7bc95cecf310ca9a7bdeee5`。该父提交当时是 J25 的 J24 层边界；“当前重排后 J24”已过时，不能代表 PR #28 合并后的 `main`。

表 1 记录旧报告在上述版本上的结果，仅作历史证据；它不证明 PR #28 合并后的集成结果，也不证明本轮 review 修复。

**表 1：旧 J25 源版本的测试结果**

| 范围 | 测试数 | 失败 | 错误 | 跳过 |
|---|---:|---:|---:|---:|
| SPI 单元测试 | 13 | 0 | 0 | 0 |
| core 单元测试 | 100 | 0 | 0 | 0 |
| HBase 单元测试 | 119 | 0 | 0 | 0 |
| J25 HBase MiniCluster | 1 | 0 | 0 | 0 |

## 3. 当前 J25 review 修复验证

PR #28 合并后，`main` 的有效 base 为 `8d6b231f6958552eacc3887abb7a865cc064c7a8`；J25 重放提交后的源 head 为 `8fc4172294fc36d553a179b10ae118c375ed2824`。本轮修复在该源 head 上验证，精确补丁由本地 J25 摘要中的 `source_commit` 和 `tested_patch_sha256` 绑定；旧报告中的计数不能替代本轮结果。

执行命令：

```sh
ETCD_HBASE_JDK21_HOME=<Temurin JDK 21 home> scripts/verify/j25.sh
```

使用依赖锁定并按 SHA-256 校验的 Eclipse Temurin 21.0.12.1+1 和 Maven 3.9.16 执行后，`clean verify -Pj25-hbase-local` 通过：SPI 13 项、core 140 项、HBase 130 项测试均为 0 failure、0 error、0 skipped。J25 报告确认必需 MiniCluster 测试执行 1 次，core read-view 必需用例执行 4 项；`HISTORY` 和 `NATIVE` 两个布局标记及更新、删除、插入、重建、历史读取和未选候选隔离证据均通过。

`j25-report.py` 共用 `evidence.java_reports` 检查真实 testcase 子节点、必需 suite/case，并保留 J25 专属布局标记检查。其本地摘要绑定源提交、测试补丁 SHA-256 和原始 JUnit 报告 SHA-256；摘要与 Maven 日志保存在忽略目录 `.artifacts/m2/j25/`，不纳入 PR。GitHub 在旧 head `8fc4172…` 上的 12 项检查均成功，但工作流步骤没有调用 `scripts/verify/j25.sh` 或执行 `M2ReadViewIT`；其中 MiniCluster job 执行的是 J24 测试，因此这不替代上述本地 J25 结果。

## 4. J25 覆盖

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

## 5. 证据边界

- MiniCluster 的物理 `HISTORY`/`NATIVE` 访问使用真实 HBase 表；控制状态、manifest repository 和 pin registry 是测试替身。因此未验证这些元数据的 HBase 持久化原子性或目标环境身份配置。
- `ReadViewPinRegistry` 是 J26 持久跨副本 pin manager 的接口边界。MiniCluster 使用测试 pin，未证明跨副本 pin 注册与 GC 的竞争安全。
- 两种布局的正确性路径都执行了；没有做足以选定生产布局的物理空间、compaction、p99 或接管成本比较。最终选择等待 J52.1。
- 没有真实目标 HBase 集群、Go client RPC、生产级恢复或多机故障证据；这些分别由后续任务验收。

## 6. 复核状态

本轮作者自审覆盖七项反馈、候选认证调用链、pin/物理读取/分页合并/历史可见性/取消清理路径及两种布局测试。维护者需在修复提交的新 head 上重新复查；旧 head 的 CI 与本地报告不表示本轮维护者复查通过。跨副本 pin/GC、目标 HBase 与正式 M2 验收仍未验证。
