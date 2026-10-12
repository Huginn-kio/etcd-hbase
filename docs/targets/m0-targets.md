# M0 验收目标初稿

本页建立生产目标的输入结构，不冻结任何生产容量或性能数字。所有尚无业务/HBase/SRE 来源的数值均标为 `missing-input` 并附补数责任角色和解除动作。`m0-public-smoke-v1` 只供可丢弃的工程验证，不能被引用为生产目标。

## 固定工程 smoke profile

Profile：[`m0-public-smoke.yaml`](../profiles/m0-public-smoke.yaml)，`scope: engineering-only`。其 1000 KV、256-byte value、并发 4、15 秒 Lease、16 桶盐等参数是小规模工程输入，不代表目标容量、吞吐、延迟或 HBase 预算。认证仅为显式 `dev-no-auth`，不满足 B03。

## 生产目标记录

| # | DEVELOPMENT_PLAN.md §6.2 指标 | 单位 | 生产目标 | profile / 来源 | 责任角色 | 状态与解除动作 | 确认记录 |
|---:|---|---|---|---|---|---|---|
| 1 | 点读 / 写 / Txn QPS 与 p99 | op/s、ms | 待确认 | B04 生产验收 profile 与生产采样 | 服务负责人、SRE | `missing-input`；区分点读、范围读、提交、续约的负载和测量窗口 | none；尚未提供 |
| 2 | key 数、value 大小分布、历史/删除比例 | 个、byte 分布、% | 待确认 | B05 业务数据分布 | 服务负责人 | `missing-input`；给出常态/峰值/增长样本及历史比例 | none；尚未提供 |
| 3 | 新集群数、盐桶数、表/Region 上限 | 个、bucket、表、Region | 待确认 | B01、B05 与 B02 目标集群 | 存储负责人、HBase 平台 | `missing-input`；提供盐 profile、集群数和 Region 预算 | none；尚未提供 |
| 4 | 全集群恢复时间与同时恢复数 | s/min、集群 | 待确认 | 恢复故障场景、数据规模 | 服务负责人、SRE | `missing-input`；给出恢复截止时间、对象数和并发恢复数 | none；尚未提供 |
| 5 | 活跃 Lease 数、续约 QPS、到期处理延迟 | 个、op/s、ms | 待确认 | B04/B05 与 Lease 故障场景 | 服务负责人、SRE | `missing-input`；给出峰值规模、续约负载和到期处理 SLO | none；尚未提供 |
| 6 | Watch 数、事件速率、事件 p99 延迟、回放窗口 | 个、event/s、ms、s | 待确认 | B04/B05 与断线/重放场景 | 服务负责人、SRE | `missing-input`；给出并发 watch、事件分布和可接受回放窗口 | none；尚未提供 |
| 7 | HBase RPC / 读写带宽 / scanner / 后台配额 | 次/s、B/s、个、配额 | 待确认 | HBase/SRE 配额 | HBase 平台、SRE | `missing-input`；分别给出应用预算及 scanner/后台预算 | none；尚未提供 |
| 8 | JVM heap / direct memory / 响应及事务字节上限 | byte | 待确认 | B02/B06 部署资源规格 | 服务负责人、平台 | `missing-input`；提供容器内存、direct memory 和最大 payload 限值 | none；尚未提供 |
| 9 | 控制行流量上限及热点预算与验收要求 | op/s、B/s、key 分布 | 待确认 | HBase 控制行设计和热点预算 | HBase 平台、服务开发 | `missing-input`；提供热点阈值、流量上限及观测验收条件 | none；尚未提供 |
| 10 | 邻近业务允许的 p99 / IO 影响 | ms、IO/s、B/s、% | 待确认 | 目标 HBase 集群现有负载基线 | HBase 平台、SRE | `missing-input`；给出邻近业务保护阈值和对照采样方法 | none；尚未提供 |
| 11 | GC / 备份时间窗口，RPO / RTO | 时间窗口、s/min、数据损失窗口 | 待确认 | B06 与业务恢复策略 | SRE、业务负责人 | `missing-input`；确认获批窗口及演练要求，并记录来源/日期 | none；尚未提供 |

这 11 行逐项对应 `DEVELOPMENT_PLAN.md` §6.2；以上都没有生产数值，不使用示例容量或 smoke 值代填。

## 五类目标的验收口径

- **容量：** KV、版本、Lease、Watch、表组和副本数，需注明常态/峰值/增长速率。
- **请求延迟：** 点读、范围读、提交、续约的 p50/p95/p99，并单列排队时间和负载构成。
- **恢复：** catalog 加载、owner/提交接管、Lease 到期处理和 Watch 追赶，记录数据规模、并发数和故障类型。
- **资源成本：** 区分逻辑操作数、RPC/CAS、业务返回字节、实际读取/写入字节、扫描预取及过滤量；能采集时记录 HBase WAL、HFile、复制和 compaction IO。
- **故障恢复与管理收益：** 覆盖重启、扩缩容、升级、备份恢复、操作步骤及获批的 RPO/RTO。性能结论只对已固定 profile 和实际证据负责。

## 输入请求摘要

1. **B01 / 存储维护者：** 给出目标盐算法、字节编码、桶数、预分区边界及已知 key 的期望桶向量。
2. **B02 / HBase 平台维护者：** 给出目标 HBase、client、Hadoop 版本，支持的 JDK 21 组合和可用验证地址。
3. **B03 / 身份平台维护者：** 说明 username/password 到 HBase 授权身份的机制、专用测试账号权限、启动/后台/跨副本/轮换约束及安全凭据引用。
4. **B04–B05 / 服务负责人：** 提供提交/读屏障/续租验收 profile，以及 KV/value/版本/请求/集群规模和预算；目标数值可继续留在后续阶段冻结。
5. **B06 / 安全、平台和 SRE：** 给出 TLS、凭据交接、部署/监控要求、GC/备份窗口及 RPO/RTO。
6. **I07–I08 / 项目维护者：** 决定许可证与来源通知要求、artifact 坐标和公开发布位置。

只登记安全凭据引用和权限类别，不把密码、token 或 keytab 内容写进目标文件、日志、命令参数或报告。未提供的事实维持 `missing-input`。
