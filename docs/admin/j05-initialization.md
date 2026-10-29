# J05 盐 profile、表候选与初始化原型

J05 提供可重复验证的公开盐候选、两种历史布局配置，以及离线 `plan` / 显式确认 `apply`。初始化器创建表结构并记录方案归属；etcd 数据、revision、token 和正式 bootstrap 状态由后续任务实现。

## 1. 盐与布局候选

**表 1 公开盐 profile**

| 项目 | 当前候选 |
|---|---|
| strategy ID / 编码版本 | `crc32-bucket-v1` / `1` |
| 输入 | 完整逻辑记录标识的原始字节；不修改调用者数组 |
| 桶号 | CRC32-IEEE 无符号值对 16 取模 |
| 盐 | 2 字节、无符号、固定宽度大端编码 |
| 预分区 | `0001`–`000f`，按 unsigned bytes 递增 |
| 向量 | [salt-profile.json](../../config/examples/j05/salt-profile.json) 中的 8 组向量，由 Java 与 Python 独立计算 |

`SaltStrategy` / `SaltProfile` 位于 JDK-only 的 `spi` 模块。所有逻辑记录类别共用完整标识输入与同一 profile；具体业务 rowkey 编码和扫描实现归 J11 等后续任务。表组路由使用 HBase profile、namespace 和表名绑定，盐接口不接收集群路由 ID。

默认 CRC32 实现将桶枚举限制在 65,536；初始化原型另将预分区限制在每表 1,024 个 region，公开示例为 16 个。扩展策略、目标 B01 规则和数据重编码需要独立验证；已有表组不能通过替换配置静默换盐。

**表 2 保留的两种历史布局**

| 示例 | 实际创建 | 候选含义 |
|---|---|---|
| [history-table.json](../../config/examples/j05/history-table.json) | `system`、`latest`、`history` | 独立当前值与历史记录表 |
| [native-cell-versions.json](../../config/examples/j05/native-cell-versions.json) | `system`、`kv` | 使用 HBase 原生 Cell 多版本的候选 |

每份配置都保留两种候选，仅创建所选布局。配置声明列族、min/max versions、TTL、KeepDeletedCells、record categories、schema/profile 版本、split keys 及权限意图。`ttl_seconds: -1` 在配置中表示不设置过期，适配器转换为 HBase 的 `FOREVER`。表级 durability 显式为 `SYNC_WAL`。

当前值、历史和系统类别分别完整声明；系统表覆盖 DECISION、EVENT、LEASE、AUTH、CONTROL。示例中的 history 单版本和 kv 多版本数值是原型参数。迟到写、删除重建、TTL/compaction、历史保留与容量对照仍归 J11/J24/J25/J52，尚未据此选择生产布局。

## 2. 配置与确认边界

初始化配置使用有界、严格的 JSON：最多 64 KiB，拒绝重复键、未知字段、缺失必填属性、null 必填值、浮点到整数或字符串到数字强制转换、数字枚举和尾随内容。内部 DTO 的集合不可变。表组引用、列族版本/TTL、权限集合、盐编码和 split keys 在接触 HBase 前校验。

同一物理 HBase namespace 中，配置的各表组候选不能重叠。loopback IPv4、IPv6、localhost、重复或重排 quorum 的等价表示会归并后检查；主机名校验不执行 DNS。`default` / `hbase` namespace 不能用于本工具。

方案指纹对已校验的 backend/profile、凭据引用、所选 schema、盐规则和配置的 namespace 表范围做有边界的 canonical JSON SHA-256。实际密码、keytab 和 token 不参与加载或序列化。变更这些配置后必须重新生成并确认方案；原型不提供既有归属的迁移或接管。

## 3. 命令用法

准备项目锁定的 JDK 21，并设置 `ETCD_HBASE_JDK21_HOME`。入口构建打包后的模块 JAR，运行 classpath 排除测试夹具、开发 classes 目录和旧版服务端 protobuf。

```bash
scripts/dev/m0-admin.sh init plan --config config/examples/j05/history-table.json
scripts/dev/m0-admin.sh init plan --config config/examples/j05/native-cell-versions.json
```

`plan` 只读取和校验配置，输出表名、列族、盐、权限意图和 `confirmation_fingerprint`，末尾标记 `backend_contacted: false`。连接地址和凭据引用不打印。多表组配置需要 `--table-group ID`；布局为 `UNDECIDED` 时需要 `--history-layout SEPARATE_HISTORY_TABLE` 或 `NATIVE_CELL_VERSIONS`，已选布局不能被命令行覆盖。

将示例的入口和 namespace 调整为获批的专用临时环境后，重新运行 `plan`，复制本次输出的完整指纹：

```bash
PLAN_FINGERPRINT='sha256:<复制本次 plan 输出的 64 位十六进制>'
scripts/dev/m0-admin.sh init apply \
  --config config/examples/j05/history-table.json \
  --confirm-table-group local \
  --confirm-plan "$PLAN_FINGERPRINT"
```

确认值不匹配时，在打开 HBase 连接前失败。`apply` 返回成功前关闭其持有的身份 provider；连接关闭失败也会作为失败报告。

**表 3 命令退出码**

| 退出码 | 含义 |
|---|---|
| 0 | 方案生成成功、表结构创建成功或同方案只读重复检查成功 |
| 2 | 参数、配置或确认值不合法 |
| 3 | 公共 CLI 尚未装配所需目标凭据/provider |
| 4 | backend、方案冲突或部分创建失败；错误不打印底层私密诊断 |

## 4. 已有表、恢复与身份

创建前校验 namespace 的表范围及已有目标表的归属。descriptor 包含实际 backend cluster ID、表组、方案指纹、schema/profile 版本、盐及 split 摘要。相同名称或列族不构成接管依据；未知 namespace 表、其他方案、其他 cluster ID 或冲突 schema 均拒绝。

- 全部目标表都已存在且 descriptor 匹配时，重复操作只读，返回 `already-provisioned`。保留已有数据和运维产生的 region splits。
- 尚有缺表时，仅允许从同一方案拥有的空表继续；已有部分表的初始 region 边界也必须匹配。空表检查使用 raw、全部版本、key-only、单行上限 Scan，包含保留历史版本及删除标记。普通 Get 读不到值不足以证明可恢复。
- 已创建的表不会因后续失败而自动删除。失败报告中的列表仅表示客户端观察到的完成项；创建 procedure 可能在客户端超时后结束。后续使用同一确认方案重新检查归属并恢复。
- 并发 namespace / table 创建会重新核对归属和初始状态。该流程不提供跨表原子事务；执行期间需要保持表组静默，不与业务写入、schema 修改或布局迁移并发。
- 同 namespace 的已配置兄弟组允许共存。仅操作当前所选组，不修改兄弟组 descriptor 或数据。

初始化 API `TableGroupInitializer.initialize(plan, confirmation, identity, connections)` 接收 J04 已验证的身份与连接工厂，核对 provider、表组和连接 profile 作用域后借用连接。连接和凭据仍由调用者的统一 provider 持有。该入口可使用 Kerberos、delegation token、账号密码 DIGEST 或自定义 provider；实际 HBase 对每个管理和读取操作继续执行权限校验。

公共 CLI 当前只装配显式 `DEV_NO_AUTH`、loopback 的开发身份。`CREDENTIAL_REFERENCE` 可以离线规划；目标凭据解析和连接安全配置未装配时，`apply` 明确返回 3。安全集成验证通过操作员提供的 J04 Kerberos keytab 身份调用上述 API，并验证无权限身份被拒绝。配置中的权限列表只是授权意图，工具不会据其自动创建账号或发放 ACL。

## 5. 验证与未覆盖项

```bash
scripts/verify/j05.sh --scope public
```

入口验证 Java 显式 import、独立盐向量、全部相关单测、J04 真实认证回归、J05 真实临时 HBase 场景和两个打包 CLI 离线方案。它要求关键用例、实际操作标记及服务器日志存在，拒绝零测试、失败和跳过。XML、日志、摘要和 SHA-256 保留在本地 `.artifacts/m0/j05/`，不提交原始报告或生成凭据。

真实场景包括两种布局、每表 16 个 region、列族实际属性、独立连接读回、重复执行的数据保留、部分失败恢复、保留删除记录拒绝、兄弟组保护，以及 Kerberos/ACL 和连接所有权。注入的部分失败发生在真实建表 RPC 之间，不代表 RegionServer 故障耐久性或生产恢复验收。

盐分散验证另启 3 个 RegionServer。初始化器创建原生 Cell 候选表组；有盐表与无盐对照表采用相同列族、durability、16 个分区边界和服务器归属。专用集群关闭 balancer，等待实际 Region 在线，再直接查询各 RegionServer 的 `RegionMetrics.getWriteRequestCount()` 增量。每批 128 个 Put，按批核对 Region/服务器归属、总写入与分散情况，并逐行 Get 读回；只统计客户端 hash 桶数不足以通过。

**表 4 固定公开数据的服务端写入分布**

| 场景 | 写入次数 | 有盐：实际 Region / 服务器 | 有盐：每 Region 写入次数 | 无盐对照 |
|---|---:|---|---|---|
| 递增的 8 字节二进制 ID | 2,048 | 16 / 3 | 每个 128 | 1 个 Region / 1 个服务器接收全部写入 |
| 相同目录前缀、不同完整 ID | 2,048 | 16 / 3 | 124–132 | 1 个 Region / 1 个服务器接收全部写入 |
| 反复更新同一逻辑 key | 128 | 1 / 1 | 单个 Region 接收全部更新，最后值可读 | 用于确认确定性盐不能拆散同一行热点 |

前两种场景各有 16 批，要求每批均命中 16 个 Region 和 3 个服务器，且单个 Region 不得占该批超过四分之一；全量分布还检查偏斜上限。采样前后的 Region 归属变化、计数重置、缺失用例或缺少多服务器日志均使验证失败。这里的写请求计数不等同网络 RPC 封包数或 CPU/I/O 压力。该探针使用 `salt || 完整 ID`，后续业务 rowkey 编码仍由 J11 等任务实现。

相同盐边界下的无盐表是编码与路由的负向对照，不是已优化无盐布局的性能基准。这里证明固定数据下的实际负载分散，不设置本机吞吐或 p99 门槛，也不推导生产容量；确定性盐不能消除单行、固定协调行或任意偏斜负载的热点。范围扫描的跨桶成本及真实业务负载仍由后续任务验证。

B01 目标盐规则仍待输入；目标身份、权限、TLS 及发行版组合沿 J04/M0 验收记录继续验证。`--scope target` 当前明确返回 3。公开候选完成、J05 PR review 和 M0 最终验收分别跟踪。
