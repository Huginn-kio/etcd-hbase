# J24 已选版本落地与提交收尾证据

**范围：** J24 公开实现与本地 MiniCluster 验证。本文不选择 M2 最终历史布局，也不代表目标 HBase 或 M2 阶段验收通过。

## 实现

- 发布 CAS 前对封存的 KV 候选逐项执行布局预检；NATIVE 版本额度不足时在发布前拒绝。
- NATIVE 版本计数仅读取 metadata/fence；校验最新版本时按其精确 timestamp 取单个完整版本，避免为计数一次加载整行历史 value。
- 选中决定按 commit sequence 归档。结果未知时强读并核对完整决定，重复创建只接受完全相同的已存记录。
- 归档后按封存目录流式读取候选，以同行 `CheckAndMutate(RowMutations)` 原子写入数据与行级 fence；相同内容重放为幂等，迟到旧 writer 不能覆盖较新 sequence。
- 全部候选确认后，才以控制行 CAS 推进 finalized sequence/revision 并清除发布槽。未实现的跨副本后台接管检查点仍由 J27 补齐。
- HISTORY 使用不可变 revision 行和单独 CURRENT 行；NATIVE 使用同一逻辑行的 Cell 多版本。两者都启用独立 fence，布局选择保持未决。

## 验证

运行入口：

```sh
ETCD_HBASE_JDK21_HOME="$JDK21_HOME" scripts/verify/j24.sh
```

JDK 21 全模块 `clean verify -Pj24-hbase-local` 成功：SPI 13 项、core 92 项、HBase 单元 106 项及 MiniCluster 集成 1 项，失败/错误/跳过均为 0。MiniCluster 用同一序列分别验证 HISTORY 与 NATIVE，包括重复落地、迟到旧 writer、删除/重建、历史版本读取、当前值读取及 NATIVE 每个版本的 fence timestamp。核心收尾测试覆盖决定已存在时的幂等归档、完成控制 CAS 后才返回 finalized，以及决定冲突时不推进 finalized sequence。最终验证报告保存在本地忽略目录 `.artifacts/m2/j24/`。

MiniCluster 提供实际 HBase client、CheckAndMutate、Cell 多版本和本地 compaction/RegionServer 证据；本次没有进行 RegionServer/WAL 故障、双副本接管或目标集群验证。该验证也不测容量、p99 或历史版本 GC。

## 布局判断与未决项

本次只验证两个候选均能完成选中版本的幂等物化。HISTORY 的写入需要分别落地历史与当前行；NATIVE 在同一行保存多个版本并受列族版本上限约束。这是由实现路径得到的操作结构差异，不是匹配负载下的成本实测。

最终选择等待 J52.1 的版本/TTL/删除标记/compaction 与读保护证据，以及 J25 对所选布局的一致读完整验证。目标 HBase 认证、权限、跨副本故障和生产成本仍待独立验收。

## 评审状态

- 作者自审：已完成；检查提交边界、原子行更新、幂等身份核对、失败关闭和 Native 版本额度。
- 独立复核：待 review chat 复核。
- 维护者正式 review：PR #28 提出后待 `reidchan` 评审。
- 目标验收和最终布局选择：未完成。
