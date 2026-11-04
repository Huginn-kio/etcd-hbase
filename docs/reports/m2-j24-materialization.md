# J24 已选版本物化与提交收尾证据

## 1. 范围与验收边界

本文记录 J24 已选提交决定归档、候选物化和 finalized 状态推进。它不选择 M2 最终历史布局，不代表目标 HBase 或 M2 阶段验收通过。

## 2. 实现

- 发布 CAS 前逐项预检封存的 KV 候选；NATIVE 版本额度不足时在发布前拒绝。
- NATIVE 版本额度检查只读 metadata/fence；校验最新版本时按精确 timestamp 读取完整版本，不为计数加载整行历史 value。
- 已选决定按 commit sequence 归档。结果未知时强读并核对完整决定；重复创建只接受完全相同的已存记录。
- 归档后从封存目录流式读取候选，并用同行 `CheckAndMutate(RowMutations)` 原子写入数据与行级 fence。相同内容重放保持幂等，迟到旧 writer 不能覆盖较新 sequence。
- 所有候选确认后，才通过控制行 CAS 推进 finalized sequence/revision 并清除发布槽。跨副本后台接管检查点由 J27 处理。
- HISTORY 使用不可变 revision 行和单独 CURRENT 行；NATIVE 使用同一逻辑行的 Cell 多版本。两种布局都启用独立 fence，最终布局选择保持未决。
- HBase 操作复用 `HBaseRequestCompletion.startRequest` 管理请求结果与 producer 实际完成；已存行的数据由 fence 校验，再与所选候选核对，避免重复复制和重建记录。
- 归档及 finalized 失败路径复用 `CompletionFailures.unwrapCompletion`；收尾与集群资源清理拆成有界步骤，前一项清理失败后仍尝试后续资源关闭。

## 3. 验证入口与本轮结果

仓库提供的 J24 完整验证入口是 `scripts/verify/j24.sh`。它使用锁定的 Temurin JDK 21，通过 `-pl hbase -am clean verify -Pj24-hbase-local` 运行 HBase 模块及其 Maven reactor 依赖；这不是全仓库构建。本轮针对 review 修复运行定向测试，没有运行该完整入口。

本轮本地验证使用锁定版本 Temurin 21.0.12.1+1：

```sh
scripts/dev/mvn21.sh -B -ntp -pl core -am \
  -Dtest=CommitFinalizationServiceTest,CommitPublisherTest \
  -Dsurefire.failIfNoSpecifiedTests=false test

scripts/dev/mvn21.sh -B -ntp -pl hbase -am -Pj24-hbase-local \
  -Dtest=HBaseCommitControlStoreCompletionTest \
  -Dit.test=M2CommitMaterializationIT \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dfailsafe.failIfNoSpecifiedTests=false verify

python3 scripts/verify/test_j24_report.py
```

结果：core 定向测试 12 项通过；HBase 请求生命周期测试 2 项和本地 MiniCluster 集成测试 1 项通过；报告验证器正反例 2 项通过。另运行 `scripts/verify/test_evidence.py`，4 项通过；两个 Maven 命令的锁定工具检查均通过。

MiniCluster 在同一序列验证 HISTORY/NATIVE、幂等重放、迟到 writer fencing、删除与重建、历史/当前读取及 NATIVE 每版本 fence timestamp。报告检查拒绝计数器与 testcase 不一致、跳过或失败的 XML。执行范围不包含全量 HBase 单测套件。

本地原始证据保存在忽略目录 `.artifacts/m2/j24/review-6632344013/`。`summary.json` 将运行绑定到基线 `27b5172fbc6909735c7d9f9f609095a5d1631b21`、实现与验证文件 patch SHA-256 `b9ab260dbe94fddcacf344173cbe276a858f78b8f6588aec1ee9eda5b173555e`，以及 MiniCluster XML SHA-256 `fe77100d2d66e7402e0be2c0100abc25c3b641e130c605f0fe004b3b01deb2a7`。该证据来自本地定向运行，不是 GitHub CI 或目标验收。

PR #28 基线 `27b5172fbc6909735c7d9f9f609095a5d1631b21` 的 GitHub 检查为 11/11 成功；它早于本次修复，不能作为本次改动的 CI 结果。本次修复推送后的 CI 仍待观察。

## 4. 布局判断

本轮 MiniCluster 只验证两个候选都能完成选中版本的物化。HISTORY 写入分别落地历史行与当前行；NATIVE 在同一逻辑行保留多个 Cell 版本，并受列族版本上限约束。这是实现路径的结构差异，不是匹配负载下的成本实测。

## 5. 未验证项

- J52.1 版本保留、TTL、删除标记及恢复比较。
- HISTORY 与 NATIVE 的显式 compaction 对照；本轮没有进行 compaction 比较。
- 与 J25 联合决定最终 M2 历史布局。
- 目标 HBase 认证、权限及跨副本、RegionServer/WAL 故障恢复。
- 本次修复后的完整 `scripts/verify/j24.sh` 全量模块验证和修复后 GitHub pipeline。
- 维护者正式 review 与 M2 最终验收。

## 6. Review 状态

- 作者自审：本轮逐项核对请求完成、结果未知处理、fence 与候选绑定、finalized 推进条件和资源关闭路径。
- 独立复核：一次有界只读复核未发现阻塞项；该复核未运行测试，测试结果来自本地定向验证。
- 维护者正式 review：Reidddddd 的反馈已处理并将在原 PR 讨论回复；维护者后续复查待定。
- 目标验收和最终布局选择：未完成。
