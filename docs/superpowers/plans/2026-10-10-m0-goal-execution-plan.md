# M0 工程、环境与协议 Implementation Plan（Agent Goal 模式）

> **For agentic workers:** 使用 `superpowers:executing-plans` 按工作包执行；用户或适用的仓库指引明确授权委派时，可以使用 `superpowers:subagent-driven-development`。所有步骤使用 checkbox 记录。执行规则服从用户指令及当前环境的 goal 工具契约。

**Goal:** 交付 M0 适用的工程、协议、实际后端接入、公开接口及验证证据，形成可评审的阶段交付包；开发任务和阶段验收状态按实际证据更新。

**Architecture:** 使用 Java 21、Maven 多模块和固定版本的 etcd proto 构建服务骨架。通用 SPI 与内部内核、HBase、gRPC 类型保持清晰边界；HBase 操作探针、协议互通和原生 etcd 对照分别记录证据。

**Tech Stack:** JDK 21；Maven Wrapper/toolchain；grpc-java/Protobuf；官方 HBase AsyncConnection/AsyncTable；Go etcd client v3.5.23；原生 etcd v3.5.23；公开 CI。

**计划版本:** 1.0 · **编写日期:** 2026-10-10（Asia/Shanghai）

**需求基线:** 根目录 [DEVELOPMENT_PLAN.md](../../../DEVELOPMENT_PLAN.md) v0.6，commit `56bef2b1248d970cd2fe41f36249d96610913ae4`，M0 的 J00–J09。执行前重新检查基线是否变化。

**本文状态:** 执行方案；本文 checkbox 均表示未来执行步骤。本轮没有开始 M0 实现，原计划仍为开发任务 0/78、阶段验收 0/9。

---

## 1. 执行范围及交付边界

M0 要获得四类结果：

1. 固定 Milvus 调用、Go client、proto、工具链和公开构建基线，产出 API 矩阵及环境/负载输入记录。
2. Maven 的 8 个模块可在 Java 21 下重复构建，生成代码、依赖检查、基础 CI 和运行镜像检查可执行。
3. 使用实际目标环境身份完成 HBase Get/Put/CAS/Scan，认证失败、权限不足、超时和连接关闭有可追溯结果。
4. 固定 Go client 能访问 Java gRPC 骨架，识别方法、请求字节、双向流终止和错误；公开 SPI、仓库规范和默认构建边界可评审。

M0 的生产 handler 对尚未实现的 API 返回 `UNIMPLEMENTED`；不能返回空 KV、虚构 revision、假 token 或默认成功。测试专用的错误/传输 fixture 必须位于 test source，并在报告中标记 `transport-fixture`。真实 HBase 探针不能被 fixture 替代。

C1 需要 J20–J25 的最小提交和一致读视图，属于后续检查点。M0 的 HBase Get/Put 与 gRPC 骨架互通不能被记录为 C1、完整 Txn、Watch 或 Lease 语义通过。提交恢复、跨副本身份/会话、正式生命周期、provider 完整装配和历史物理选型分别交给 M2、J50/J51、J55/J56、J18/J19、J11/J24/J25/J52。

## 2. Goal 启动与恢复协议

### 2.1 可直接复制的启动指令

```text
请按 docs/superpowers/plans/2026-10-10-m0-goal-execution-plan.md 在 goal 模式执行 M0。

先读 DEVELOPMENT_PLAN.md 的第 2、3、4、7、8/M0、9、10 节及执行工作树适用的 AGENTS.md。
检查当前 goal；已有同目标 goal 时继续使用，不重复创建。没有 active goal 时，创建一个覆盖本计划的 goal：
“完成 etcd-hbase M0 适用的实现和验证：Java 21 可重复构建、固定调用/协议基线、公开 SPI、盐/表候选与初始化原型、实际 HBase 身份及基础操作、Go client 协议/流/错误互通、公开 CI 和可评审交付包。实际后端证据缺失时不宣布 M0 达标；实现及验证齐全后提交可评审结果，外部评审状态如实保留。”

只使用这一个 goal，工作包是同一 goal 的步骤，不创建嵌套 goal，不自行设置 token_budget。
复用适合的隔离工作树；保护主工作区已有改动。按工作包依赖选择当前可执行步骤。
每次继续先读取 docs/progress/m0-execution.md 和本轮报告，确认已完成步骤及剩余输入，避免重复工作。
每个工作包先实现最小可验证交付，运行其指定检查，再更新报告和执行记录。
缺少真实身份、盐规则、版本或访问条件时，记录具体输入问题并请求补充，同时继续其他无依赖步骤。
不以 mock、开发无认证环境或仅编译成功替代真实目标后端、Go 互通和阶段证据。
任务状态按实现/验证/评审事实更新；没有真实评审结果时保持“待评审”。
范围限定在 M0；遇到 M1/M2/M5 的依赖时交付契约和衔接记录，不能提前实现完整内核。
本次执行授权本地开发、测试及文档更新；共享环境写入只使用已确认的专用测试资源。
推送、PR、发布和部署依照当次用户授权执行，不将它们作为本地交付的隐含前置条件。
```

“用户要求输出本方案”只授权编写文档。只有用户发送执行指令后，执行者才启动上述 goal。

### 2.2 工具使用约定

| 当前情况 | Agent 行为 |
|---|---|
| 没有未完成 goal，且用户已要求执行 | `create_goal` 的 objective 使用上述具体目标；用户没给 token 预算时省略 `token_budget` |
| 已有同目标 goal | `get_goal` 读取状态，继续读取执行记录；不能重新 create |
| 已有其他未完成 goal | 保留其状态，请用户明确目标切换；不把原 goal 擅自标为 complete/paused |
| 本地实现和适用验证齐全，可评审交付包已生成 | 可把“可评审交付”goal 标为 `complete`；报告明确外部评审是否完成，不能据此勾选 M0 验收 |
| 缺少必需输入，但其他工作包仍可推进 | 工作包记为 `等待输入`，goal 保持运行；继续无依赖工作 |
| 相同输入问题连续至少 3 个 goal turn 阻塞，已经没有可推进工作 | 按当前工具契约把 goal 标为 `blocked`，列出输入、解除条件和已经完成的交付 |
| 用户明确要求暂停 | 按工具契约标为 `paused` 并停止；执行者不能自行暂停 |
| 预算耗尽 | 保存进度、剩余步骤和证据；不以预算接近耗尽为理由标为完成 |
| 环境没有 goal 工具 | 按同一进度文件逐步执行，明确工具不可用；不能虚构 `codex goal` CLI 或 goal 状态 |

goal 状态、工作包状态、原计划主任务状态和阶段验收是四个不同对象。goal 的 `blocked` 规则使用工具规定的 turn 次数；工作包可以在第一次发现缺少输入时记录 `等待输入`。

用户恢复曾经 blocked 的 goal 时重新开始阻塞次数审计，不沿用之前的三次计数。预算由用户明确指定且目标达成时，从 `update_goal` 返回结果报告最终 token 使用量。`complete` 的对象是本文明确的可评审交付：必需实现和目标后端证据必须齐全；正式外部评审及未授权的远程 CI 执行不被伪装为已完成。

### 2.3 每个执行 turn 的最小循环

1. 读取 goal、工作树 status、执行记录及最近一次报告；确认没有用户的新约束。
2. 选择依赖全部成立的最小未完成步骤。优先解除 Java/依赖、proto 和实际身份风险。
3. 实现并运行指定检查；检查失败时记录命令、退出码和具体错误，修复后只重复受影响检查。
4. 更新工作包证据；在 `DEVELOPMENT_PLAN.md` 更新对应原任务的实际跟踪字段。
5. 保存下次可直接接续的步骤和剩余输入。任务已验证时整理独立提交；执行授权允许提交才提交。
6. 持续工作期间至少每 60 秒给出一次有内容的进展；工具长操作分段等待，避免失去沟通。

## 3. 工作树、文件与状态约定

文档中的文件路径均相对执行工作树根目录。不要把当前机器绝对路径写入 POM 或公开配置。

执行者先使用 `list_artifacts` 查找合适的现有工作树；需要新工作树时优先使用 `create_worktree`，明确 ref 为开发计划基线。新工作树不复制未提交文件，因此必须把本文复制到同一路径并核对 SHA-256。若当前工作树为 detached HEAD，在干净起点上建立 `codex/m0-engineering` 分支；不重置任何用户文件。

主要文件布局：

```text
pom.xml                                      父 POM、版本和依赖管理
mvnw / mvnw.cmd / .mvn/                      Wrapper、Java toolchain 模板
protocol/                                    vendor、Java 生成及 descriptor 校验
spi/                                         通用扩展契约和不可变 DTO
core/                                        请求上下文、内部错误及未来内核边界
hbase/                                       官方 client 的连接适配和基础探针
grpc/                                        服务骨架、Header/Error 工厂和拦截器
server/                                      骨架启动与集成测试编排
admin/                                       空表组初始化原型
testkit/                                     Java 契约工具、Go 驱动和对照器
config/examples/m0/                          公开开发候选配置
docs/api-matrix.md                           J00.2 能力矩阵
docs/milvus-etcd-usage.md                     J00.1 调用证据
docs/baselines/m0-baseline.json               版本、commit、来源、hash
docs/inputs/m0-inputs.yaml                    外部输入和值的证据状态
docs/profiles/m0-public-smoke.yaml             有限工程 smoke profile
docs/targets/m0-targets.md                    J00.3 目标初稿及数据来源
docs/architecture/m0-module-boundaries.md      模块/SPI/内部 port 边界
docs/architecture/m0-auth-hbase.md             实际身份适配和后续生命周期约束
docs/storage/m0-salt-profile.md                盐 profile、向量和预分区
docs/storage/m0-table-candidates.md            两种历史表布局候选
docs/build/m0-toolchain.md                    JDK、依赖、冲突和运行参数
docs/protocol/m0-wire-baseline.md              descriptor、method、字段和错误基线
docs/progress/m0-execution.md                  工作包步骤和恢复入口
docs/reports/m0/<run-id>/                     可提交的脱敏报告及证据索引
scripts/dev/                                 工具链、Java/Go/etcd 开发启动脚本
scripts/verify/                              本计划中的验证入口
.github/workflows/m0.yml                      公开构建与公开集成验证
.artifacts/m0/<run-id>/                       忽略的原始输出/临时服务/工具下载
```

`run-id` 在 WP00 生成，例如 `20261010T080000Z-56bef2b`；实际报告使用执行时的 UTC 时间和基线 commit，不复制示例值。报告展示日期时标注 Asia/Shanghai。运行产物及本地凭据不能提交。

原计划是 J00–J09 状态及计数的唯一来源。本文 checkbox 和进度文件只跟踪 WP 步骤；不要再复制一张带主任务完成计数的看板。原任务在实现、适用验证、真实评审全部齐全后才勾选；跨阶段残留子交付仍保留未完成记录。

## 4. 基线、默认 profile 与输入规则

### 4.1 已核对的公开候选基线

| 项 | 本版依据 | 执行规则 |
|---|---|---|
| 开发计划 | v0.6，`56bef2b1248d970cd2fe41f36249d96610913ae4` | 执行前重新读差异，范围变更记入报告 |
| etcd proto 与原生对照 | v3.5.23，commit `1cf4bc40111fd95d1e14a90a09a146357b15eb13` | vendor 和对照进程都使用固定 commit/版本及校验摘要 |
| Milvus 公开审计候选 | v2.6.18，commit `5aee61bff0de2c9650af81f2e9b0bef44efe13fe` | WP01 审计后形成正式调用基线；用户给定其他版本时重新审计 |
| Go client/api/server | 上述 Milvus go.mod 为 v3.5.23 | client/pkg、grpc-go 等也记录实际 MVS 结果及 replace |
| Milvus Go 声明 | `go 1.25.8` | testkit 使用该兼容基线；记录实际 Go patch 和 module graph |
| Java | 21，无 preview | WP02 锁定公开可获取发行版/patch；构建、CI、运行镜像统一 |
| HBase/client/Hadoop | 原计划尚未冻结 | WP02/WP15 获取目标输入并验证；不凭 Java 版本猜组合 |
| 公共 HBase 候选 | 官方矩阵将 2.6.8+ / 2.7 列为 JDK 21 推荐组合 | 选定一个公开稳定、制品可获取的具体版本，仍需真实连接及操作验证；目标集群组合单独记录 |

本机在 2026-10-10 的只读检查发现：默认 `java`/`javac` 指向 Java 8，装有 Oracle JDK 21.0.4；Maven、Go、protoc 可找到，Docker 未找到。此记录仅帮助 WP00 快速发现风险，不能作为执行时状态或最终发行工具链。

### 4.2 工程 smoke 的确定值

WP05 创建以下 `docs/profiles/m0-public-smoke.yaml`。这些值仅用于有限工程验证，不能填入生产验收目标：

```yaml
profile_id: m0-public-smoke-v1
scope: engineering-only
protocol_version: 3.5.23
kv_count: 1000
value_bytes: 256
request_concurrency: 4
request_timeout_ms: 5000
lease_count: 10
lease_ttl_seconds: 15
watch_count: 4
salt_profile:
  id: crc32-be16-b16-v1
  algorithm: crc32-ieee
  buckets: 16
  salt_width_bytes: 2
native_etcd:
  listen_host: 127.0.0.1
  persistence: disposable-test-directory
  ports: allocated-by-runner
hbase:
  environment: disposable-public-dev
  authentication: explicit-dev-no-auth
  namespace_prefix: etcd_hbase_m0_
```

公开候选盐 profile 不自动关闭 B01；开发无认证 profile 不自动关闭 B03。缺少 Docker 时可使用固定源码/制品启动原生 etcd，以及固定 HBase test utility 的真实临时集群；记录 HBase 进程的 JVM 与应用 client 的 JVM，不混为同一证据。

### 4.3 外部输入及局部阻塞

| 输入 | 必需内容 | 影响步骤 | 缺少时继续执行的内容 |
|---|---|---|---|
| B01 | 目标盐算法、字节编码、桶数、预分区和校验样例 | WP07 的目标 profile、WP17 目标初始化 | 公开 profile、向量、候选配置和 dry-run |
| B02 | 目标 HBase/client/Hadoop 版本、JDK 21 组合和测试地址 | WP02 目标组合、WP15/WP16 | 工程、协议、公开组合和公开集成环境 |
| B03 | username/password 到实际 HBase 身份的机制、专用测试账号权限、安全凭据引用、跨副本/后台/轮换约束 | WP15/WP16 真实身份及 target probe | SPI、公开 dev-no-auth、错误链路、协议骨架 |
| B04 | 提交/读屏障/续租验收 profile | WP05 目标记录；正式负载归 M2/M4 | 目标初稿和有限 smoke |
| B05 | KV/value/版本/请求/集群数规模与预算 | WP05 目标记录、后续 M1/M7 | 工程和协议验证 |
| B06 | TLS、凭据交接、测试环境规范、部署监控、RPO/RTO | WP15/WP16 安全接入、运行约束 | 不包含真实凭据的公开验证 |
| I07 | 维护者的许可证决定及来源记录 | WP04 最终 LICENSE/NOTICE | 贡献、模板、来源清单和候选说明 |
| I08 | artifact 命名、发布位置及维护者决定 | WP04 公开制品规则 | 使用本地 `io.etcdhbase:etcd-hbase-parent:0.1.0-SNAPSHOT` 构建 |

每个缺失值使用以下结构，明确解除动作而非留下未定义步骤：

```yaml
id: B03
state: missing-input
required_fact: username/password 如何建立目标 HBase 的授权身份
value: null
source: none
responsible_role: HBase身份平台维护者
affected_steps: [WP15.target, WP16.target]
resolution: 提供认证机制说明、专用测试权限和安全凭据引用；随后执行目标身份与操作探针
```

只记录凭据引用和权限类别。密码、token、keytab 内容不写入该文件、报告、POM、YAML、命令行参数或指标。Kerberos/UGI 等机制必须由目标平台证据确定；不能把配置 username/password 当成标准 HBase 已完成密码认证。

## 5. 任务覆盖与依赖

| 原任务 | 工作包 | M0 验收后的衔接 |
|---|---|---|
| J00.1/J00.2/J00.3 | WP01/WP12/WP05 | M6 按真实部署再核对消费者和 Count |
| J01 | WP02/WP03/WP10/WP22 | 正式运行包及 K8s 生命周期归 J56 |
| J02 | WP09/WP10 | 生成代码由工具维护 |
| J03 | WP18/WP19/WP21 | 完整业务 handler 随 M3–M5 实现 |
| J04.1/J04.2 | WP15/WP16 | Authenticate 会话与身份轮换归 J50/J51 |
| J05.1/J05.2/J05.3 | WP07/WP08/WP17 | 物理布局归 M1/M2/J52；正式 bootstrap 归 J55 |
| J06.1/J06.2/J06.3 | WP13/WP14 | 已发布提交的恢复上下文必须在 J27 联动验证，记录跨阶段残留 |
| J07 | WP11/WP20/WP21 | 并发历史及完整等价对照归 J28/M3–M7 |
| J08 | WP06/WP15 | 默认 provider、加载与跨版本契约归 J18/J19/J58 |
| J09 | WP04/WP22 | 公开 Quickstart、制品与发布归 J59/J84/J85 |

执行顺序索引（可执行性以列出的子依赖为准）：

```text
WP00
 ├─ WP01 调用审计 ────────── WP12 能力矩阵
 ├─ WP02 工具版本 ─ WP03 Maven
 │                    ├─ WP06 SPI ─ WP07 盐 ─ WP08 表配置
 │                    │                 └──────── WP13 配置 ─ WP14 上下文/指标
 │                    │                                  └─ WP19 骨架
 │                    └─ WP09 vendor ─ WP10 生成 ─ WP11 Go基础 ─ WP20 原生对照
 │                                             └─ WP18 错误/Header ─ WP19 ─ WP21 互通
 ├─ WP04 开源规则
 └─ WP05 输入/目标 ─ B02/B03/B06就绪 ─ WP15 身份 ─ WP16 HBase操作
                                WP08/WP13/WP16.public ─ WP17 初始化
 已具备的公开交付 ─ WP22 CI/运行检查
 全部适用实现及验证 ─ WP23 可评审交付包
```

WP15 的接口设计、WP16 的公开探针、WP17 的 dry-run 和 WP22 的公开 CI 可在目标输入缺失时推进；它们的 target 子步骤单独记录。所有工作包默认由当前 agent 顺序执行；本表不自动授权启动子 agent。

## 6. 分步工作包

每个工作包按“读取证据 → 明确失败用例 → 最小实现 → 指定验证 → 保存证据”执行。代码步骤有多个文件时逐个实现；一个 checkbox 应可在一个短执行片段内推进，复杂子步骤拆成自己的 checkbox，不能一次积累整阶段改动。

### WP00：建立可恢复的执行起点

**关联：** 全部任务。**依赖：** 用户已要求执行。

**文件：** 创建 `docs/progress/m0-execution.md`、`docs/inputs/m0-inputs.yaml` 初始输入记录、`.gitignore`；不修改主工作区 `.idea/`。

- [ ] 读取适用指引、原计划和本文；执行 `git status --short --branch`、`git rev-parse HEAD`、`git worktree list`，记录初始改动和基线。
- [ ] 建立/复用隔离工作树和 `codex/m0-engineering` 开发分支；复制执行方案并记录 hash。
- [ ] 读取当前 goal，按第 2 节建立或恢复一个目标。
- [ ] 检查 Java/Javac/Maven/Go/protoc、网络和测试访问条件；记录实际版本，不自动安装全局软件。
- [ ] 生成 run-id，并把 `.idea/`、所有模块 `target/`、`.artifacts/`、本地 credential/toolchain 文件加入忽略规则。
- [ ] 创建进度文件，写入当前 WP、下一步骤、输入问题、最近报告路径和主计划位置。

**验证命令：**

```bash
git status --short --branch
git rev-parse HEAD
git check-ignore .idea/ .artifacts/m0/probe.txt protocol/target/probe.txt
```

**预期：** 每个示例运行路径被忽略；只出现授权的新文件，已有改动不丢失。**证据：** `WP00.md`。**下一步：** WP01/WP02/WP04/WP05。

### WP01：固定公开 Milvus 调用与版本证据

**关联：** J00.1。**依赖：** WP00。

**文件：** `docs/baselines/m0-baseline.json`、`docs/milvus-etcd-usage.md`；公开源码放在忽略的 `.artifacts/m0/sources/milvus/`。

- [x] 按第 4.1 节固定 Milvus commit；从公开源码获取 go.mod/go.sum、replace、etcd client 及 grpc-go 的实际版本。
- [x] 用 `rg` 逐条审计原计划 §13.1 的文件/符号，继续搜索 `clientv3`、`WithPrefix`、`WithRange`、`WithRev`、`CountOnly`、`WithLimit`、`PrevKV`、`KeepAlive`、`Watch`、`Compact` 和错误消费；排除只用于历史迁移的入口并记录排除依据。
- [x] 每个调用记录 commit、源码路径/符号、区间、选项组合、响应消费者、错误/重试分支和验证 case ID。
- [x] 记录 native etcd commit、客户端版本及公开部署假设。认证、Kafka、Count 的边界引用原计划。
- [x] 复查 catalog、session/主备、resolver/proxy、配置、Streaming、CDC、TSO/ID、管理入口都有证据；发现吞错时登记风险和后续 J66，不在本工作包修改 Milvus。

**证据格式：**

```text
usage_id | commit | path:symbol | method | key/range表达式 | options
response_fields | error/retry_consumer | case_ids | first_release_disposition
```

**验证：** 对源码执行与上述选项相同的搜索，逐项与清单交叉检查；`git -C .artifacts/m0/sources/milvus rev-parse HEAD` 必须等于 baseline。没有 source anchor 的项目不能标为“已审计”。**证据：** `WP01.md`。**下一步：** WP11、WP12。

### WP02：选择并记录工具和依赖的具体版本

**关联：** J01、B02。**依赖：** WP00；目标组合按输入就绪程度推进。

**文件：** `docs/build/m0-toolchain.md`、`build/dependency-lock.json`、`docs/baselines/m0-baseline.json`。

- [ ] 读取官方 Java/HBase 支持矩阵及目标 B02；把“公开开发组合”和“目标验证组合”分开记录。
- [ ] 从公开 release 和 Maven Central 选择并锁定 Java 21 发行版/patch、Maven 3.9 发行版、Wrapper/toolchains/compiler/enforcer/surefire/failsafe/protobuf 生成插件。公共发行工具链默认选择 Eclipse Temurin 21 HotSpot 的具体 GA patch；在公开下载和运行镜像都有确定来源后锁定，不把本机 Oracle JDK 的存在当作发行版决定。
- [ ] 锁定 protoc、grpc-java、protobuf-java、HBase client/test utility、Hadoop、Netty、Guava、SLF4J、测试库与 JSON/YAML 库；只加入当前工作包实际需要的库。
- [ ] 每项保存 group/artifact、具体版本、来源、校验摘要、选择依据；禁止 `LATEST`、版本范围、依赖生产 SNAPSHOT 或仅运行时下载的未锁定工具。
- [ ] 检查 Java 21 非 preview；记录所需 JVM 参数的对应原因，不批量加入无依据的 `--add-opens`。

**锁文件必需字段：** `jdk.vendor/release/archives/sha256`、`maven.version/distributionSha256/wrapperPlugin`、`plugins`、`libraries`、`nativeTools`、`hbase.public/target`、`go.version/moduleVersions`、`sourceEvidence`。缺少 target 的值以第 4.3 节输入记录表示；公开构建所需值必须全部是确定版本。

**完成判断：** 公开版本与制品可获取且有摘要；真正的 JDK/HBase 组合验证继续由 WP16/WP22完成，不能在本工作包宣称兼容通过。**证据：** `WP02.md`。

### WP03：建立 Maven 8 模块及 Java 21 入口

**关联：** J01 的工程子交付。**依赖：** WP02 的公开锁文件。

**文件：** 根/8 个模块的 `pom.xml`、`mvnw`、`mvnw.cmd`、`.mvn/wrapper/maven-wrapper.properties`、`.mvn/toolchains.xml.example`、`build/maven-settings-public.xml`、`scripts/dev/configure-toolchain.py`、`scripts/dev/mvn21.sh`。

- [ ] 先建立父 POM 和 `protocol/spi/core/hbase/grpc/server/admin/testkit` 模块。统一开发坐标 `io.etcdhbase`、版本 `0.1.0-SNAPSHOT`，发布坐标由 WP04 记录维护者结论。
- [ ] 把 WP02 的版本填入 `dependencyManagement` 和 `pluginManagement`；根 reactor 关系为 `core→spi`、`hbase→core/spi`、`grpc→protocol/core/spi`、`server→grpc/hbase/core`、`admin→hbase/spi`、`testkit→protocol/spi`。测试依赖不制造生产依赖环。
- [ ] 使用固定 Wrapper 插件生成脚本，加入 distribution checksum；公开构建不读取扩展工程或私有仓库。
- [ ] 配置 `release=21`、toolchain 21、Enforcer Java 21 和依赖收敛；测试 JVM 也使用同一 toolchain。
- [ ] `mvn21.sh` 使用任务专用变量解析 JDK 路径，校验主版本后以该 JDK 运行 Wrapper；不依赖调用者当前 `java`，不改全局 shell 配置。
- [ ] 配置 `.mvn/toolchains.xml.example` 和 `configure-toolchain.py`，在忽略的路径生成实际 toolchain 文件，并由 Wrapper 显式 `--toolchains` 读取；公开 POM 不出现本机路径。
- [ ] 公共 settings 使用 Maven Central，不继承用户私有 repository/profile；脚本显式选择该 settings。首次公开复现使用忽略目录中的独立 local repository，记录它与日常开发缓存的区别。

**父 POM 必须包含的确定配置：**

```xml
<properties>
  <maven.compiler.release>21</maven.compiler.release>
  <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
</properties>
<!-- 插件及依赖的具体 version 从 WP02 锁文件写入，不使用范围。 -->
```

`configure-toolchain.py` 的确定实现如下。路径由 XML 库转义；它只写指定的忽略路径，不改用户的全局 toolchain：

```python
#!/usr/bin/env python3
import argparse
from pathlib import Path
import xml.etree.ElementTree as ET

parser = argparse.ArgumentParser()
parser.add_argument("--jdk-home", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()
jdk_home = Path(args.jdk_home).expanduser().resolve()
output = Path(args.output).expanduser().resolve()
if not (jdk_home / "bin" / "java").is_file():
    parser.error("jdk-home must contain bin/java")
if output == (Path.home() / ".m2" / "toolchains.xml").resolve():
    parser.error("output must be a project-local ignored file")
root = ET.Element("toolchains")
toolchain = ET.SubElement(root, "toolchain")
ET.SubElement(toolchain, "type").text = "jdk"
provides = ET.SubElement(toolchain, "provides")
ET.SubElement(provides, "version").text = "21"
configuration = ET.SubElement(toolchain, "configuration")
ET.SubElement(configuration, "jdkHome").text = str(jdk_home)
output.parent.mkdir(parents=True, exist_ok=True)
ET.ElementTree(root).write(output, encoding="utf-8", xml_declaration=True)
```

toolchain 文件不提交。父 POM 的 toolchain 约束必须与 `<provides><version>21</version>` 一致。

**`scripts/dev/mvn21.sh` 的入口行为：**

```bash
#!/usr/bin/env bash
set -euo pipefail
M0_REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
M0_JDK_HOME="${ETCD_HBASE_JDK21_HOME:-}"
if [ -z "$M0_JDK_HOME" ] && [ "$(uname -s)" = Darwin ]; then
  M0_JDK_HOME="$(/usr/libexec/java_home -v 21)"
fi
if [ -z "$M0_JDK_HOME" ] || [ ! -x "$M0_JDK_HOME/bin/java" ]; then
  printf '%s\n' 'Set ETCD_HBASE_JDK21_HOME to the locked JDK 21 installation.' >&2
  exit 2
fi
M0_JAVA_SPEC="$("$M0_JDK_HOME/bin/java" -XshowSettings:properties -version 2>&1 | awk '/java.specification.version =/ {print $3}')"
if [ "$M0_JAVA_SPEC" != 21 ]; then
  printf '%s\n' 'The selected JDK must be Java 21.' >&2
  exit 2
fi
M0_TOOLCHAINS_XML="${ETCD_HBASE_TOOLCHAINS_XML:-$M0_REPO_ROOT/.artifacts/m0/toolchains.xml}"
python3 "$M0_REPO_ROOT/scripts/dev/configure-toolchain.py" --jdk-home "$M0_JDK_HOME" --output "$M0_TOOLCHAINS_XML"
exec env JAVA_HOME="$M0_JDK_HOME" PATH="$M0_JDK_HOME/bin:$PATH" "$M0_REPO_ROOT/mvnw" \
  --toolchains "$M0_TOOLCHAINS_XML" \
  --settings "$M0_REPO_ROOT/build/maven-settings-public.xml" "$@"
```

该入口先检查 major；WP22 还要按锁文件校验 vendor/patch，避免仅 major 相同就声称工具链一致。

**验证命令：**

```bash
scripts/dev/mvn21.sh -version
scripts/dev/mvn21.sh -B -ntp validate
scripts/dev/mvn21.sh -B -ntp clean verify
scripts/dev/mvn21.sh -B -ntp dependency:tree
```

**预期：** 8 个模块构建通过、日志显示 Java 21、无 preview；空模块没有业务测试时如实记为工程检查。显式用 Java 8/17 执行 Wrapper `validate` 必须失败并指出基线要求。**证据：** `WP03.md`。

### WP04：开源规则、许可证输入及制品命名

**关联：** J09。**依赖：** WP00；构建规范随 WP03 补全。

**文件：** `CONTRIBUTING.md`、`CHANGELOG.md`、`.github/PULL_REQUEST_TEMPLATE.md`、`.github/ISSUE_TEMPLATE/bug_report.yml`、`docs/build/open-source-policy.md`、`THIRD_PARTY_NOTICES.md`；维护者确认后创建 `LICENSE`、`NOTICE`。

- [ ] 写入 Jxx/task ID、实现/验证/评审证据和阶段计数规则；PR 模板不能把“已编译”当作“已完成”。
- [ ] 记录 Apache-2.0 为原计划候选，请维护者确认 I07；没有决定时保留候选和明确输入问题，不擅自宣布仓库许可证。
- [ ] 记录 vendored proto、第三方依赖和工具的来源、许可证及所需通知；WP09 增加真实文件来源后复核。
- [ ] 记录公共依赖、无扩展环境 CI、公开配置、artifact 名称及候选发布位置；维护者决定 I08 后冻结公开规则。
- [ ] 明确 M0 只检查本地 package 和公开构建；公开发布由 J85 执行。

**验证：** 模板含 task/证据字段；通知条目可追溯到实际 source；维护者决定有记录；公开文件不存在环境专用地址、凭据或私有制品依赖。**证据：** `WP04.md`。

### WP05：建立输入与五类验收目标初稿

**关联：** J00.3、B01–B06。**依赖：** WP00。

**文件：** `docs/inputs/m0-inputs.yaml`、`docs/profiles/m0-public-smoke.yaml`、`docs/targets/m0-targets.md`。

- [x] 写入第 4.2 节有限 smoke profile；验证数值和用途标签齐全。
- [x] 为原计划 §6.2 每个指标建立目标记录：容量、请求延迟、恢复、资源成本、故障恢复与管理收益；含单位、profile、来源、负责人角色、状态和确认记录。
- [x] 缺少真实数据时填写 `missing-input` 及具体补数动作；不把 1000 KV、示例 10000 Lease 或当前机器容量写成生产目标。
- [x] 建立 B01–B06、I07/I08 与受影响 WP 的映射；汇总一次简明输入请求。
- [x] 将仍可启动的工作包写入执行记录；J01/J02/J03/J07 不等待全部容量目标。

**验证：** 目标类别覆盖原计划每行、单位明确、示例和真实目标区分；不存在没有解除动作的缺失值。**证据：** `WP05.md`。

### WP06：通用 SPI 与内部边界

**关联：** J08。**依赖：** WP03。

**文件：** `docs/architecture/m0-module-boundaries.md`；`spi/src/main/java/io/etcdhbase/spi/salt/{SaltStrategy,SaltProfile}.java`、`spi/src/main/java/io/etcdhbase/spi/identity/{BackendIdentityProvider,BackendCredentialInput,AuthenticationScope,AuthorizedBackendIdentity,BackendIdentityException}.java`；`spi/src/test/java/io/etcdhbase/spi/SpiBoundaryTest.java`、`IdentityContractTest.java`。

- [ ] 写出 8 模块依赖图；通用 `spi` 不引用 gRPC、HBase 或 core 实现类型。HBase 特有连接工厂放在 `hbase` 的明确公开 API 包。
- [ ] 定义盐/身份的最小接口；provider 接口不 sealed，ID 显式，失败、取消与关闭契约明确。
- [ ] 定义 `SaltProfile` 的算法/版本/桶数/宽度，DTO 校验及防御性复制；定义身份的 subject、opaque contextRef、credentialGeneration、允许表组集合，不能包含明文密码/token。
- [ ] `BackendCredentialInput` 使用受控可关闭对象，复制输入、避免自动生成泄漏密码的 `toString`、关闭时清理持有内容；记录 Java String/堆副本的实际限制。
- [ ] 在 `SpiBoundaryTest` 验证禁止引用类型和依赖；`IdentityContractTest` 验证失败分类、不可变授权集合、取消、关闭及凭据对象不输出秘密。
- [ ] 将配置/指标/端口绑定列为待装配验证的候选，不提前扩大稳定 API；内核提交状态、ReadView、fencing、revision 等保持内部类型。

**接口形状：**

```java
public interface SaltStrategy {
    String id();
    SaltProfile profile();
    byte[] salt(byte[] logicalIdentity);
    java.util.List<byte[]> splitKeys();
}

public interface BackendIdentityProvider extends AutoCloseable {
    String id();
    java.util.concurrent.CompletionStage<AuthorizedBackendIdentity> authenticate(
        BackendCredentialInput credentials, AuthenticationScope scope);
    @Override
    void close();
}
```

上述类型全部由本工作包定义并说明字段/生命周期。第三方 provider 不能通过这些接口跳过内核不变量。**验证命令：** `scripts/dev/mvn21.sh -B -ntp -pl spi -am test`。**预期：** 两个契约测试实际执行；报告读取 surefire XML 的测试数及结果，不能用“0 tests”作为通过。**证据：** `WP06.md`。

### WP07：盐候选 profile、确定向量与目标规则

**关联：** J05.1。**依赖：** WP06；target 子步骤依赖 B01。

**文件：** `spi/src/main/java/io/etcdhbase/spi/salt/HashSaltStrategy.java`、`spi/src/test/java/io/etcdhbase/spi/salt/HashSaltStrategyTest.java`、`docs/storage/m0-salt-profile.md`、`config/examples/m0/salt-profile.json`。

- [ ] 使用 `java.util.zip.CRC32` 对完整逻辑 identity 字节计算 IEEE CRC32；unsigned 结果 `% 16`，输出固定 2 字节大端桶号。
- [ ] 先实现向量测试，再实现公开候选策略；禁止默认字符集、字符串路径归一化或截断目录深度。
- [ ] `splitKeys()` 返回桶 1..15 的两字节前缀，unsigned byte 顺序严格递增；返回数组防御性复制。
- [ ] 测试盐桶和盐宽非法配置、长 identity、修改输入/输出数组、所有记录类别都能使用同一契约；业务 rowkey 完整编码归 J10。
- [ ] 获取 B01 后增加目标 profile 和同样的测试向量；如与公开候选不同，分别保存版本与适用环境，不能在线悄悄更改已有表组编码。

**确定测试向量（hex）：**

| identity bytes | CRC32 | bucket | salt |
|---|---|---:|---|
| 空字节串，仅测试盐函数 | `00000000` | 0 | `0000` |
| `61` | `e8b7be43` | 3 | `0003` |
| `2f612f622f632f` | `639fc949` | 9 | `0009` |
| `00` | `d202ef8d` | 13 | `000d` |
| `ff` | `ff000000` | 0 | `0000` |
| `6100ff` | `9b569788` | 8 | `0008` |
| `2f726f6f74` | `b203698f` | 15 | `000f` |
| `2f726f6f742f` | `04664f0c` | 12 | `000c` |

**验证命令：** `scripts/dev/mvn21.sh -B -ntp -pl spi -am test`。**预期：** 向量、预分区、非法值和复制测试均执行；公开候选通过不等于 B01 关闭。**证据：** `WP07.md`。

### WP08：两种表配置候选及校验规则

**关联：** J05.2。**依赖：** WP07.public；目标配置按 B01/B02。

**文件：** `docs/storage/m0-table-candidates.md`、`config/examples/m0/history-table.yaml`、`config/examples/m0/native-cell-versions.yaml`。

- [ ] 分别描述 `latest/history/system` 和 `kv/system` 候选，保持逻辑 current/history/decision/event/lease/auth/control 状态的要求，不声明物理选型已完成。
- [ ] 每个表保存 schema/profile 版本、CF、versions、TTL、split keys、记录类别和身份权限；端口绑定表组，不把配置表组 ID 编入 rowkey。
- [ ] 独立 history 候选使用每 key/revision 的不可变记录；原生 Cell 候选记录显式 timestamp/revision 的待验证关系。
- [ ] 公开临时样本表不启用墙钟 TTL：history 候选各 cell `maxVersions=1`，原生 Cell 候选 `maxVersions=64` 仅用于 <=64 版本的样本；正式版本保留/GC 决策归 J11/J24/J25/J52。
- [ ] 记录迟到写、删除/重建、列间一致性、max/min versions、TTL 和 compaction 必测项；不能用 timestamp 上界当作提交证明。

**验证：** 两种配置都可被 WP13 的 schema 读取；split keys 与 WP07 一致；报告明确样本参数、选型状态和后续任务。**证据：** `WP08.md`。

### WP09：vendor etcd proto 与完整 import 依赖

**关联：** J02。**依赖：** WP03。

**文件：** `protocol/src/main/proto/etcd/api/etcdserverpb/rpc.proto`、`etcd/api/mvccpb/kv.proto`、`etcd/api/authpb/auth.proto`；同根 `gogoproto/gogo.proto`、`google/api/annotations.proto`、`google/api/http.proto` 及递归所需 import；`protocol/upstream-lock.json`、`protocol/UPSTREAM.md`、`scripts/verify/vendor-proto.py`、`THIRD_PARTY_NOTICES.md`。

- [ ] 从固定 etcd commit 获取原始 proto，保留 proto package、service 名称和字段编号；递归解析 import。
- [ ] 按该 etcd 基线的依赖锁定获取 gogo/Google API import；记录 source repo、commit、path、license、SHA-256，不下载浮动 branch。
- [ ] 原始 vendor 文件保持字节校验；Google well-known descriptor 由锁定 protoc 工具提供并记录版本。
- [ ] `vendor-proto.py` 检查 import 闭包、摘要、重复定义和來源；缺文件/摘要变化时返回非零，不静默联网修复。
- [ ] 若 Java 编译必须做 options 适配，保留原始文件和可复现转换脚本；只允许 Java codegen options 变化，WP10 对 package/service/字段/枚举/oneof/stream flags 严格比对。

**验证命令：** `python3 scripts/verify/vendor-proto.py --lock protocol/upstream-lock.json --root protocol/src/main/proto`。**预期：** 所有项目有确定来源、hash 和 import；破坏一个副本的摘要可在临时目录中得到非零结果。**证据：** `WP09.md`。

### WP10：Java 生成、descriptor 和字段守卫

**关联：** J02、J01。**依赖：** WP09、WP02 的生成工具版本。

**文件：** `protocol/pom.xml`、`protocol/src/test/java/io/etcdhbase/protocol/ProtocolDescriptorTest.java`、`ProtocolBinaryRoundTripTest.java`、`protocol/baseline/wire-contract.json`、`docs/protocol/m0-wire-baseline.md`、`scripts/verify/protocol.sh`；生成结果仅在 `protocol/target/`。

- [ ] Maven 调用锁定的 protoc/grpc-java generator；生成 Java 消息、service stub 和包含 import 的 descriptor set。应用 proto 及 gogo/Google API 支持类型必须有 Java 类型，well-known protobuf 类型复用运行库，不重复生成。
- [ ] 初版保留 vendor 的 Java 默认 package/outer class：例如 `etcdserverpb.KVGrpc`、`etcdserverpb.Rpc.RangeRequest`。若实际生成结果不同，先核对 source/options 并记录；后续引用以生成代码为准。
- [ ] 从原始上游 descriptor 建立 wire-contract；测试比较 proto package、完整 service/method、消息字段的编号/类型/label、enum 数值、oneof 和 client/server streaming flags。
- [ ] 对 `RangeRequest` 的 bytes/range/revision/options、Txn 的嵌套 oneof、Watch 的 create/cancel/progress、LeaseKeepAlive 和 Auth 字段做 binary round-trip；覆盖 `00/ff` 字节和 >2^53 的 int64。
- [ ] 生成后清理 `target/` 再构建，核对生成流程可以重现；禁止手改生成 Java 解决依赖问题。
- [ ] 测试发现 package/字段/stream flags 变化必须失败；只允许记录过的 Java codegen options 差异。

**验证命令：**

```bash
scripts/dev/mvn21.sh -B -ntp -pl protocol -am clean verify
scripts/verify/protocol.sh
```

**预期：** upstream descriptor 和 Java descriptor 一致；方法包括 `etcdserverpb.KV/Range`、`etcdserverpb.Watch/Watch`、`etcdserverpb.Lease/LeaseKeepAlive`、`etcdserverpb.Auth/Authenticate`。生成文件不出现在 git status。**证据：** `WP10.md`。

### WP11：Go client 驱动和无损历史记录格式

**关联：** J07 的驱动基础。**依赖：** WP01、WP10。

**文件：** `testkit/go/go.mod`、`testkit/go/go.sum`、`testkit/go/cmd/m0driver/main.go`、`testkit/go/internal/trace/{record.go,record_test.go}`、`testkit/go/internal/client/options.go`、`testkit/trace-schema.json`、`scripts/dev/go-m0.sh`。

- [ ] go.mod 锁定 Go 与 etcd/client/pkg/api/grpc-go 的审计结果；执行 `go list -m all` 核对 MVS 后的有效版本和 replace。
- [ ] 建立 `go-m0.sh`：使用锁定 Go、`GOTOOLCHAIN=local`，版本不符明确失败；本地下载只放忽略目录，不能静默升级全局 Go。
- [ ] 实现 trace 读写：bytes 为 base64，int64/uint64 为十进制字符串，时间和 elapsed 不参与协议等价比较；格式包含 case/request/response/error/stream terminal。
- [ ] Authenticate name/password、响应 token 和 authorization metadata 永远不写 trace；只记录字段是否存在及成功/错误分类。增加输入/响应/异常的凭据泄漏负例，不能先序列化整条 protobuf 再期望调用方脱敏。
- [ ] 使用 `clientv3` 和必要的原始 apiv3 stub；每个请求有 deadline，记录消费者实际错误和 gRPC status 两个视角。
- [ ] 先通过 `record_test.go` 验证二进制 key、大 revision、oneof、PrevKV、空值、顺序、错误和 truncated JSONL；出现格式错误返回非零，不能忽略一行继续通过。

**trace 的最小完整结构示例：**

```json
{"schema":"etcd-hbase-trace/v1","case_id":"range-bytes","ordinal":1,"method":"etcdserverpb.KV/Range","request":{"key_b64":"YQD/","range_end_b64":"","revision":"0"},"response":null,"error":{"grpc_code":"Unimplemented","consumer_kind":"grpc-status"},"stream_terminal":null,"evidence_scope":"transport","elapsed_ns":"1000000"}
```

完整 schema 还需覆盖 header、KV 元数据、Count/More、Txn 分支、Lease、Watch 事件序列及 PrevKV；不能因本例只有错误就省略将来的响应字段。JSON 中不存在凭据或 token。

**验证命令：**

```bash
scripts/dev/go-m0.sh -C testkit/go test ./internal/trace ./internal/client
scripts/dev/go-m0.sh -C testkit/go list -m all
```

**预期：** 无损格式测试通过，module graph 与基线一致；当前只证明驱动/记录工程。**证据：** `WP11.md`。

### WP12：完整首版能力和消费者矩阵

**关联：** J00.2。**依赖：** WP01、WP10；case ID 与 WP11 对齐。

**文件：** `docs/api-matrix.md`、`testkit/cases/m0-wire.json`、`testkit/count-policy.json`、`scripts/verify/api-matrix.py`。

- [ ] 以 descriptor 枚举服务/方法及字段，并以 WP01 的 usage_id 标注消费者和组合；没有调用证据的条目也要明确首版承诺/拒绝结果。
- [ ] 每行记录 method、request options、response fields、error/重试消费者、首版 disposition、对应 Jxx、case ID、当前实现阶段。
- [ ] 覆盖 Range、Put/Delete、Txn nested/duplicate/range compare、Lease 管理、Watch progress/filter/fragment、Auth 管理、Compact、MemberList/Status/Alarm，以及其他逐项 unsupported RPC。
- [ ] 完整写入六类 Count 规则；Count 放行条件按请求组合限制，More、deleted、Version/CreateRevision/ModRevision、Txn 和 Watch 不能被豁免。
- [ ] `api-matrix.py` 检查 descriptor method 覆盖、case ID 唯一、usage 引用可解析、每个拒绝项有明确错误；不存在“默认全支持”。

**验证命令：** `python3 scripts/verify/api-matrix.py --matrix docs/api-matrix.md --wire-contract protocol/baseline/wire-contract.json --cases testkit/cases/m0-wire.json`。

**预期：** 零缺失 method、零悬空 case/usage；首版范围固定但实现状态仍由后续任务更新。**证据：** `WP12.md`。

### WP13：运行配置 schema 与端口/表组校验

**关联：** J06.1。**依赖：** WP03、WP08。

**文件：** `server/src/main/java/io/etcdhbase/server/config/{ServerSettings,SettingsLoader,SettingsValidator}.java`、`server/src/test/java/io/etcdhbase/server/config/SettingsValidatorTest.java`、`config/schema/server.schema.json`、`config/examples/m0/server-skeleton.yaml`。

- [ ] 定义明确的配置优先级：文件后应用环境变量覆盖，随后统一校验；记录允许键和映射，不接受拼错的未知字段。
- [ ] 绑定 listener→table group、schema/profile 版本、provider ID、请求 deadline 和 scanner/RPC/字节/队列上限；所有预算是正值并有单位。
- [ ] `skeleton` 使用 loopback/随机端口；`dev-no-auth` 必须显式启用。实际认证配置只接受 credential reference，拒绝 YAML 中的明文密码/token。
- [ ] 测试端口重复、表组/profile 不一致、未知键、预算非法、provider 缺失、明文凭据、重复表名；错误不得原样回显敏感内容。
- [ ] schema 与 Java loader 使用同一配置集合；报告字段差异并修复，不能仅验证 YAML 能解析。

**验证命令：** `scripts/dev/mvn21.sh -B -ntp -pl server -am test`。**预期：** `SettingsValidatorTest` 实际执行；每种非法配置清晰失败，合法候选可加载。**证据：** `WP13.md`。

### WP14：请求上下文、取消、日志和有限类别指标

**关联：** J06.2/J06.3。**依赖：** WP13、WP06。

**文件：** `core/src/main/java/io/etcdhbase/core/request/RequestScope.java`、`core/src/main/java/io/etcdhbase/core/error/BackendFailure.java`、`core/src/test/java/io/etcdhbase/core/request/RequestScopeTest.java`、`grpc/src/main/java/io/etcdhbase/grpc/RequestScopeInterceptor.java`、`grpc/src/test/java/io/etcdhbase/grpc/RequestScopeInterceptorTest.java`、`server/src/main/java/io/etcdhbase/server/observability/RequestMetrics.java`、`server/src/test/java/io/etcdhbase/server/observability/RequestMetricsTest.java`、`server/src/main/resources/logback.xml`。

- [ ] 请求上下文包含 monotonic deadline、取消和幂等 cleanup 注册；CompletionStage 主路径的资源持有者明确。
- [ ] 定义不依赖 HBase/gRPC 的 `BackendFailure`：分类包括 authentication、permission、unavailable、timeout、cancelled、resource-limit、unknown，并保留 cause；日志格式不直接输出可能含凭据的原始 message。HBase 探针在 WP16 使用它，gRPC 投影在 WP18 使用它。
- [ ] 用可控 future/时钟验证取消早于/晚于完成、重复 close、超时、异步异常和 cleanup 抛错；所有已登记资源恰好释放一次，避免只测试正常路径。
- [ ] gRPC context 取消映射到内部 scope；`finally` 关闭 scope 不丢失原始失败原因。
- [ ] 日志只记录方法类别、状态、延迟、预算和 trace ID；指标维度使用有限枚举。key、账号、密码、token、表组动态名称不作为 metric label。
- [ ] 定义请求 scope 与服务恢复 scope 的所有权边界；记录“已发布提交不得因客户端取消丢失”的 J27 联动用例，但不在 M0实现发布协议。

**验证命令：** `scripts/dev/mvn21.sh -B -ntp -pl core,grpc,server -am test`。**预期：** 取消/释放测试及日志/指标测试执行；J06.3 的未来发布恢复用例明确保留。**证据：** `WP14.md`。

### WP15：实际后端身份机制与连接工厂

**关联：** J04.1、J08；target 依赖 B02/B03/B06。**依赖：** WP06、WP05，基础代码依赖 WP03。

**文件：** `docs/architecture/m0-auth-hbase.md`、`hbase/src/main/java/io/etcdhbase/hbase/api/HBaseConnectionFactory.java`、`hbase/src/main/java/io/etcdhbase/hbase/identity/DevelopmentIdentityProvider.java`、`hbase/src/main/java/io/etcdhbase/hbase/connection/OfficialHBaseConnectionFactory.java`、`hbase/src/test/java/io/etcdhbase/hbase/identity/IdentityIsolationTest.java`；目标 adapter 的类名和支持机制先在报告确定，再创建对应类。

- [ ] 读取目标安全机制和 client 源码，确认 Authenticate 的 name/password 怎样得到授权 HBase 上下文；区分认证失败、表组权限不足、后端不可用。
- [ ] 公共开发 provider 只用于显式 dev-no-auth；target 路径必须使用经过验证的标准机制或目标 provider。机制无法映射时保持输入/适配阻塞，不发伪成功身份。
- [ ] `createAsyncConnection` future完成不能单独证明认证和权限；identity 的实际权限范围须由 WP16 对专用目标表的访问证据确认。探针输入使用与 Authenticate 一致的 name/password 凭据对象，不引入替所有请求使用固定启动账号的生产路径。
- [ ] HBase 特有工厂接口使用锁定 HBase client 类型，位于 `hbase.api`，通用 `spi` 仍无 HBase依赖。
- [ ] 按表组、subject、credentialGeneration 管理有界连接复用；不按请求新建连接。测试不同身份不共享未证明等价的连接/缓存，关闭和失败后资源可回收。
- [ ] 对跨副本引用、全部 Pod 重建时激活材料、owner 转发/后台恢复身份、凭据轮换建立约束清单，逐项映射 J50/J51/J55/J56。
- [ ] 记录实际身份流程、支持范围、网络/TLS、授权测试资源和证据；账号密码不进入 token/业务表/manifest/日志/文件。

**Java 工厂的最小形状：**

```java
public interface HBaseConnectionFactory extends AutoCloseable {
    java.util.concurrent.CompletionStage<org.apache.hadoop.hbase.client.AsyncConnection>
        connectionFor(io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity);
    @Override
    void close();
}
```

工厂必须说明借用连接的关闭权归管理器；调用者关闭请求不能关闭共享连接。credentialGeneration 变化和进程关闭分别验证。**验证：** `scripts/dev/mvn21.sh -B -ntp -pl hbase -am test`；目标身份测试由 WP16 执行。**证据：** `WP15.md`。**阻塞结果：** 实际身份机制不明时，只推进 public 子步骤并请求 B03。

### WP16：真实 HBase Get/Put/CAS/Scan 与故障证据

**关联：** J04.2。**依赖：** WP15、WP08、WP14；target 还依赖 B02/B03/B06。

**文件：** `hbase/src/main/java/io/etcdhbase/hbase/probe/BackendProbe.java`、`hbase/src/test/java/io/etcdhbase/hbase/probe/LocalHBaseProbeIT.java`、`TargetHBaseProbeIT.java`、`hbase/pom.xml`、`scripts/verify/hbase-probe.sh`。

- [ ] 创建专用 `etcd_hbase_m0_<run-id>` namespace/table，记录权限及资源清单；禁止复用业务表。探针只操作自己创建的记录。
- [ ] 使用 AsyncConnection/AsyncTable：Put 两个含 `00/ff` 的原始 key/value；Get 比较原始字节；CAS 正确基值返回成功、过时基值返回 false；Scan 用明确边界验证顺序和 stop row。
- [ ] 从另一个连接读取确认后端持久状态；服务连接关闭/重建后继续读取同样字节，不能仅比对本地缓存。
- [ ] public profile 启动真实临时 HBase，必须明确无认证；target profile 使用 WP15 的实际授权身份。
- [ ] 在专用测试环境验证错误凭据、权限不足、后端不可达/超时和取消；检查 Async exception 解包到分类/调用者的链路，不能返回 empty/false 表示故障。
- [ ] 不具备某种 target 故障权限时记录该验证受阻；transport-fixture 的错误测试不能替代目标身份负例。
- [ ] 关闭 scanner/future/连接并核对资源释放；清理只删除本轮记录的专用 namespace/table。失败时保留可恢复资源清单。

**验证命令：**

```bash
scripts/verify/hbase-probe.sh --profile public-local
scripts/verify/hbase-probe.sh --profile target --config-ref "$ETCD_HBASE_M0_TARGET_CONFIG"
```

`ETCD_HBASE_M0_TARGET_CONFIG` 由执行者提供本地受控配置引用，不含命令行密码。脚本内部调用对应 Maven Failsafe profile，设置 `failIfNoTests=true`；缺少配置返回非零及 `missing-input`，不能 skip 后绿灯。

**预期：** public 和 target 分别有版本/身份/操作/故障/关闭证据。public 不关闭 B03；target 必需证据缺失时 J04保持待验证/阻塞，M0 阶段不可通过。**证据：** `WP16-public.md`、`WP16-target.md`。

### WP17：空表组初始化 CLI 原型

**关联：** J05.3。**依赖：** WP08、WP13、WP16.public；target apply 依赖目标输入/权限。

**文件：** `admin/src/main/java/io/etcdhbase/admin/InitializeTablesCommand.java`、`admin/src/test/java/io/etcdhbase/admin/InitializeTablesCommandTest.java`、`InitializeTablesIT.java`、`scripts/dev/m0-admin.sh`、`docs/storage/m0-init-cli.md`。

- [ ] `init plan` 读取候选配置，打印脱敏的表/CF/profile/schema/split 计划；不接触业务数据。
- [ ] `init apply` 只创建已确认的专用空表组，记录创建状态；schema/profile 不一致或已有业务记录时拒绝变更。
- [ ] 重复执行相同计划必须无破坏；模拟创建到一半失败后重试，已创建且一致的对象可继续，冲突对象明确失败。
- [ ] 测试独立 history 和原生 Cell 两种候选；不得在 apply 中自动选择正式历史方案。
- [ ] 报告正式 schema bootstrap、恢复和升级由 J55/J57完成，不以 CLI 原型代替它们。

**验证命令：**

```bash
scripts/dev/mvn21.sh -B -ntp -pl admin -am verify -Pm0-hbase-local
scripts/dev/m0-admin.sh init plan --config config/examples/m0/history-table.yaml
scripts/dev/m0-admin.sh init plan --config config/examples/m0/native-cell-versions.yaml
```

**预期：** dry-run、两种候选、重复和部分失败恢复用例均执行；target apply 仅在输入与专用资源确认后运行。**证据：** `WP17.md`。

### WP18：Header/Error 工厂和原生错误语义

**关联：** J03。**依赖：** WP10、WP12、WP06。

**文件：** 使用 WP14 已定义的 `BackendFailure`；创建 `grpc/src/main/java/io/etcdhbase/grpc/protocol/{ResponseHeaderFactory,ErrorProjection,AuthenticateCredentialMapper}.java`、`grpc/src/test/java/io/etcdhbase/grpc/protocol/{ResponseHeaderFactoryTest,ErrorProjectionTest,AuthenticateCredentialMapperTest}.java`、`docs/protocol/m0-error-map.md`。内部错误相关子步骤还依赖 WP14。

- [ ] Header 工厂接受明确的 cluster/member/revision/term 输入；测试 int64/uint64 边界，不内置“当前 revision=1”或虚构 Raft 状态。
- [ ] 从固定 Go `rpctypes` 和 native etcd 结果建立错误基线；逐项记录 status code、canonical message、sentinel/消费行为。
- [ ] 保留认证失败与权限不足的不同语义；不能把所有认证错误简单映射为 `UNAUTHENTICATED`。超时、取消、服务不可达、资源准入失败分别测试。
- [ ] `AuthenticateCredentialMapper` 将生成的 AuthenticateRequest 转成 WP06 凭据对象，授权 scope 由端口绑定确定；测试字段映射和脱敏，不生成 token。与 WP16 输入对象一致，真正 Authenticate会话成功路径留给 J50.1。
- [ ] HBase adapter 产生内部失败类型，grpc 层不依赖 HBase exception；CompletionException 等包装保留原始分类。
- [ ] 未实现方法统一返回 gRPC `UNIMPLEMENTED`；不得因为请求解码成功返回成功 header。

**骨架中可复用的确定返回：**

```java
public static io.grpc.StatusRuntimeException notImplemented(String method) {
    return io.grpc.Status.UNIMPLEMENTED
        .withDescription("etcd-hbase M0: " + method + " is not implemented")
        .asRuntimeException();
}
```

**验证命令：** `scripts/dev/mvn21.sh -B -ntp -pl grpc -am test`。**预期：** Header 数值/字段测试、错误表测试执行；后端不可用不会投影成“key 不存在”。**证据：** `WP18.md`。

### WP19：可启动 gRPC 骨架和流终止

**关联：** J03，J56.1 的最小工程输入。**依赖：** WP13、WP14、WP18。

**文件：** `grpc/src/main/java/io/etcdhbase/grpc/service/{KvService,WatchService,LeaseService,AuthService,ClusterService,MaintenanceService}.java`、`grpc/src/test/java/io/etcdhbase/grpc/service/SkeletonServiceTest.java`、`server/src/main/java/io/etcdhbase/server/Bootstrap.java`、`server/src/test/java/io/etcdhbase/server/BootstrapTest.java`、`scripts/dev/m0-server.sh`。

- [ ] 扩展生成的 ImplBase 并注册所有矩阵内服务；尚未实现的 unary 返回 WP18 的错误，不进行业务 KV 写入。
- [ ] 对 Watch/KeepAlive 实现接收/终止/取消生命周期：接受正确 protobuf 请求后明确终止为 UNIMPLEMENTED，输入异常和重复终止不泄漏 scope。
- [ ] Bootstrap 按 WP13 配置选择固定 listener/table group，支持 loopback 端口 0，记录真实绑定端口；配置非法时非零退出。
- [ ] 骨架数据 readiness 明确为未实现；未来 Authenticate 和数据 readiness 分离的约束保持在配置/启动设计中，不实现虚假认证 token。
- [ ] 测试 unary 方法完整路径、二进制请求到达、stream 请求类型、取消、shutdown 和启动失败；gRPC Reflection 不作为 smoke 前置条件。

**验证命令：**

```bash
scripts/dev/mvn21.sh -B -ntp -pl grpc,server -am verify
scripts/dev/m0-server.sh --config config/examples/m0/server-skeleton.yaml
```

**预期：** 真实端口可连接、骨架请求得到 UNIMPLEMENTED、SIGTERM/显式 shutdown 回收资源；不把此结果记录为 KV/Lease/Watch 已实现。**证据：** `WP19.md`。

### WP20：原生 etcd 场景、对照器及拒绝掩盖测试

**关联：** J07。**依赖：** WP11、WP12。

**文件：** `testkit/go/cmd/native-etcd/main.go`、`testkit/go/internal/scenario/{scenario.go,native_test.go}`、`testkit/go/internal/compare/{compare.go,compare_test.go}`、`testkit/scenarios/m0-native.yaml`、`scripts/dev/native-etcd.sh`、`scripts/verify/native-oracle.sh`。

- [ ] 使用锁定的 `go.etcd.io/etcd/server/v3/embed` 启动独立测试进程，绑定 loopback 分配端口，等待 ReadyNotify，并将所有数据置于本轮忽略目录。记录其 Go build info、模块版本和分发形态；不声称验证了未运行的上游镜像。
- [ ] 原生场景包括：二进制 Put/Get、Prefix/Limit/More/CountOnly、Compare 的两个分支、删除及 PrevKV、Grant/KeepAlive/Revoke、列表 R→Watch R+1、错误和 Compact 后旧 revision。
- [ ] 为 Lease/Watch 等异步场景使用真实 deadline 和事件条件等待，不用固定 sleep 判断成功；场景结束关闭流、client和原生进程。
- [ ] 对照器只允许条件化 Count 差异及必要的 ID 对应。revision 使用场景基准的统一相对映射，保留顺序、同事务归属和所有字段引用关系；不能逐响应随意归一化。
- [ ] 用受控 trace 副本分别删除 Watch 事件、改变 More/deleted/revision/Txn 分支、改变 PrevKV 字节、截断 stream；每种必须使 compare 返回非零。
- [ ] driver 给原生 etcd 的两次独立运行建立可重复对照。Java 正向业务等价在 M3/M4/M5 加入，当前 Java 骨架不能被对照器放行。

**验证命令：**

```bash
scripts/dev/go-m0.sh -C testkit/go test ./internal/compare
scripts/verify/native-oracle.sh --scenario testkit/scenarios/m0-native.yaml
```

**预期：** 原生真实场景完成、两次运行一致、所有损坏样本被拒绝。native 输出、合法 normalization 和失败差异均保留。**证据：** `WP20.md`。

### WP21：Go 到 Java 的协议/流/错误互通

**关联：** J03、J07 的 L2 基础。**依赖：** WP19、WP20；不等待 WP16.target。

**文件：** `server/src/test/java/io/etcdhbase/server/GoInteropIT.java`、`server/src/test/java/io/etcdhbase/server/fixture/FailureFixture.java`、`testkit/go/internal/wire/{probe.go,probe_test.go}`、`scripts/verify/wire.sh`。

- [ ] 由 Java 集成测试启动真实骨架端口，再运行固定 Go driver；不手工使用可能被其他服务占用的 2379。
- [ ] 高层 clientv3 和原始 apiv3 stub 分别测试 method 路径、raw bytes、大 int64、请求 deadline；记录哪一层实际发出了调用。
- [ ] 为 Watch/KeepAlive 各发送正确 request，核对 Java 接收类型和 Go 收到的 terminal status；验证取消后 scope、stream 和子进程清理。
- [ ] test-only `FailureFixture` 按固定错误基线注入认证失败、权限不足、不可达和超时，Go 端验证 status/sentinel；它不提供业务成功值，也不作为真实后端错误证明。
- [ ] 验证所有缺失能力都明确拒绝，客户端重试不会使 smoke 无界等待；每个 case 有真实成功/失败退出判断。
- [ ] 输出 `transport`、`native-etcd`、`target-hbase` 三类证据的对应索引；不能把前两类写成 target认证通过。

**验证命令：** `scripts/verify/wire.sh --cases testkit/cases/m0-wire.json`。

**预期：** 所有 L2 基础 cases 有 trace、status 和端点信息；测试专用 fixture 有清晰范围；双向流不等于完整 Watch/KeepAlive 语义。**证据：** `WP21.md`。

### WP22：公开 CI、依赖收敛和 Java 21 运行检查

**关联：** J01、J09。**依赖：** WP03、WP04 的公开规则、WP10、WP17.public、WP20、WP21；目标版本锁定补充依赖 WP16.target。

**文件：** `.github/workflows/m0.yml`、`scripts/verify/m0.sh`、`scripts/verify/report-index.py`、`server/src/main/docker/Dockerfile.m0`、`server/pom.xml`、`docs/build/m0-toolchain.md`、`build/dependency-lock.json`。

- [ ] CI 固定 Java 21 vendor/patch、Go、Maven Wrapper 和下载摘要，第三方 Action 使用完整 commit SHA；不访问扩展仓库、私有包源或目标环境凭据。
- [ ] 配置 build/proto/Go oracle/wire/public HBase jobs，使用同样的 scripts；HBase local job 缺依赖或没有执行测试时失败，不能用 skip 形成绿灯。
- [ ] 记录 Maven effective POM/dependency tree、Netty/Guava/protobuf/Hadoop 冲突及排除原因；禁止通过关闭全部收敛检查解决单个冲突。
- [ ] 用 package 后的实际 classpath启动 server skeleton；不只运行 IDE/test classpath。runtime image 用锁定的公开 Java 21 image digest，校验 Java 版本及骨架启动/退出。
- [ ] Docker 本地不可用时继续本机 Java runtime、CI配置和等价脚本检查，记录镜像运行验证缺口；具备运行器后补运行，不宣称镜像已通过。
- [ ] 具备推送/PR 授权时记录真实 CI run URL 与 commit；没有该授权时交付可审查 workflow 和本地同命令证据，实际 GitHub CI 状态明确为未运行。
- [ ] 目标组合通过 WP16 后补入最终依赖/运行支持矩阵；public 与 target 如有差异，各自锁定并解释支持范围。

**验证命令：**

```bash
scripts/dev/mvn21.sh -B -ntp clean verify
scripts/dev/mvn21.sh -B -ntp help:effective-pom -Doutput=.artifacts/m0/effective-pom.xml
scripts/dev/mvn21.sh -B -ntp dependency:tree
scripts/verify/m0.sh --scope public
```

`m0.sh` 顺序调用已定义的 protocol、native oracle、wire、public HBase 等脚本，任一步非零即整体失败。summary 必须列出实际测试数量及 skipped。报告使用真正 run-id，不能覆盖前一轮失败证据。

**预期：** public 结果可重现；Java vendor/patch、依赖图和各 job 状态有证据。workflow 存在、本地通过、镜像通过、GitHub job通过分别记录。**证据：** `WP22.md`。

### WP23：M0 可评审交付包及阶段状态核对

**关联：** 全部 J00–J09。**依赖：** 所有适用实现、检查和真实后端输入就绪；缺失输入保留为阻塞。

**文件：** `docs/reports/m0/<run-id>/INDEX.md`、`M0_REVIEW_PACKAGE.md`、`docs/progress/m0-execution.md`、`DEVELOPMENT_PLAN.md`、`README.md` 的实际工程入口说明。

- [ ] 对原计划的每个 M0 完成标准建立实现文件/commit、命令/结果、报告及评审状态映射。
- [ ] 核对实际身份/操作、目标盐规则、版本和输入记录；开发无认证与候选参数不替代必需 target 证据。
- [ ] 核对 8 模块、wire descriptor、Go method/stream/error、Count 边界、SPI 依赖、开源规则、真实运行与 CI状态。
- [ ] 在主计划更新任务跟踪及阶段计数，只统计有实现、适用验证、真实评审的主任务；报告跨阶段子交付及后续 Jxx。
- [ ] 生成 reviewer可直接读取的证据索引：变更、测试范围、未覆盖项、风险、输入决定、后续边界；自查和外部评审分别署名/记录，不能伪造评审人。
- [ ] 本地目标工作、M0必需 target 验证和许可证/制品规则输入全部达成时，交付可评审结果并完成该 goal；外部评审、真实 CI/镜像的尚缺结果如实保留，M0验收仍按原计划退出条件判定。B04/B05的生产数值与B06的后续运维细项按原计划依赖推进，只要求M0目标初稿及本轮安全接入条件完整，不把M2/M7目标冻结提前为M0前置。
- [ ] 必需实现或 target验证受阻时，保留 goal 未完成，按第 2 节输入/blocked规则结束当前可推进链路；给出精确恢复入口。

**验证命令：**

```bash
git diff --check
scripts/verify/m0.sh --scope public
scripts/verify/hbase-probe.sh --profile target --config-ref "$ETCD_HBASE_M0_TARGET_CONFIG"
python3 scripts/verify/report-index.py --phase M0 --progress docs/progress/m0-execution.md
```

已经通过且无相关新改动的长检查可引用同 commit 的本轮结果，不无理由重跑。主计划没有拿到的审核/CI/运行证据不能靠重新运行本地测试补成“已通过”。**证据：** `WP23.md` 和 review package。

## 7. 标准验证入口与创建责任

执行时先完成创建责任对应工作包，再运行命令。不得把尚未存在的脚本当作现有能力。

Maven profile 明确设置如下，避免一次 `verify` 意外访问目标集群：默认运行 `*Test`；`m0-hbase-local` 只在 hbase/admin 启用 LocalHBaseProbeIT/InitializeTablesIT；`m0-hbase-target` 只在 hbase 启用 TargetHBaseProbeIT；`m0-wire` 只在 server 启用 GoInteropIT。Failsafe 的 `failIfNoTests=true` 在启用了对应 IT execution 的模块配置，不能施加到无 IT 的其他模块。public/target脚本分别指定 profile，日志和 summary核对实际 case数量。

| 入口 | 创建工作包 | 检查对象 | 非零退出条件 |
|---|---|---|---|
| `scripts/dev/mvn21.sh` | WP03 | Java 21 Wrapper 入口 | JDK不符/构建失败 |
| `scripts/dev/configure-toolchain.py` | WP03 | 项目本地toolchain XML | JDK路径不合法/无法写入指定路径 |
| `scripts/verify/vendor-proto.py` | WP09 | source/import/hash | 摘要或依赖不一致 |
| `scripts/verify/protocol.sh` | WP10 | 生成/descriptor/二进制字段 | 字段、method、stream或测试缺失 |
| `scripts/dev/go-m0.sh` | WP11 | 固定 Go 与 module graph | 版本不符/Go命令失败 |
| `scripts/verify/api-matrix.py` | WP12 | method/usage/case 覆盖 | 缺项/悬空/不明确拒绝 |
| `scripts/verify/hbase-probe.sh` | WP16 | public/target 真实操作 | 缺输入/认证或操作失败/未执行测试 |
| `scripts/dev/m0-admin.sh` | WP17 | 初始化计划/专用环境apply | 非空/配置冲突/权限错误 |
| `scripts/dev/m0-server.sh` | WP19 | 骨架实际 classpath启动 | 配置/启动/Java版本失败 |
| `scripts/dev/native-etcd.sh` | WP20 | 原生测试服务 | 版本/启动/ready超时 |
| `scripts/verify/native-oracle.sh` | WP20 | 原生场景和对照器 | trace错误/差异被错误放行 |
| `scripts/verify/wire.sh` | WP21 | Go/Java互通 | case失败/无限等待/清理失败 |
| `scripts/verify/m0.sh` | WP22 | 公开集成检查组合 | 任何子检查失败或测试被跳过 |
| `scripts/verify/report-index.py` | WP22 | 报告结构、case和结果索引 | 缺报告/缺退出码/引用不存在 |

所有 runner 记录起止时间、版本、端口、资源清单和进程身份。终止时只操作自己启动且身份核对一致的进程；清理不能使用 `killall java` 或删除整个用户工作目录。脚本打印路径/摘要及简明失败原因，避免输出全量敏感配置。

## 8. 完成判定及阶段交接

### 8.1 证据判定表

| 已有结果 | 可以声称 | 仍需保留的状态 |
|---|---|---|
| Maven clean verify通过 | 对应commit的构建和实际执行测试通过 | HBase目标接入、CI、镜像未运行项 |
| public HBase临时集群通过 | 官方client在公开临时环境完成基础操作 | B02/B03目标组合与身份仍未证实 |
| target HBase实际身份和操作通过 | 对应身份/表组/版本的基础访问已验证 | 跨副本token/后台/轮换归J50/J51 |
| Go/Java unary/stream错误互通通过 | 基础wire/流终止/错误投影已验证 | C1/KV/Txn/Lease/Watch业务语义未交付 |
| 原生oracle及对照器损坏样本拒绝通过 | 对照工具和native测试基线可用 | Java完整等价留至对应业务阶段 |
| 独立Java运行通过、Docker缺失 | 本机Java运行已验证 | 镜像运行待验证 |
| CI配置与本地同命令通过 | 公开workflow和本地检查可评审 | GitHub实际job未运行 |
| 全部M0适用实现及必需环境验证齐全 | 本地可评审goal可完成 | 原任务保持待评审，阶段门按真实审核/运行结果更新 |

### 8.2 M0 正式退出清单

- [ ] JDK 21 工具链、依赖锁、构建/CI/运行的实际状态和版本一致性有证据。
- [ ] 固定调用/版本基线、首版能力矩阵、Count消费者与拒绝项完整。
- [ ] 目标HBase实际认证及Get/Put/CAS/Scan完成，错误/取消/关闭可追踪。
- [ ] 盐规则、测试向量、预分区及两种表配置候选有记录，目标profile输入明确。
- [ ] Go client最小method/请求字节/stream/error互通完成；没有把骨架当成业务API。
- [ ] SPI、内部port、公开依赖与默认构建边界有实现/验证/评审。
- [ ] 许可证、来源通知、仓库/制品规则有维护者结论和实际文件。
- [ ] 目标初稿、外部输入、未关闭风险及跨阶段残留子交付记录完整。
- [ ] 主计划中状态、计数、证据和实际评审结果一致；M0阶段验收有真实结论。

进入M1时优先使用M0产出的profile、codec边界、连接工厂和错误链路；进入C1还必须补提交/读视图。原生Cell版本方案保持候选，不能因配置文件已经建表而冻结布局。

## 9. 执行记录与报告模板

### 9.1 `docs/progress/m0-execution.md`

```markdown
# M0 执行记录

Goal objective：本计划第2.1节
执行工作树：实际绝对路径
开发分支与基线commit：实际值
计划SHA-256：实际值
本轮run-id：执行时生成值
当前WP及checkbox：当前最小步骤
下一条可执行命令：具体命令
已验证WP与证据：工作包ID、commit、报告路径
等待输入：输入ID、受影响子步骤、解除动作
跨阶段残留：原任务ID和接收阶段
最近失败：命令、exit、原因、修复/复跑状态
主任务状态来源：DEVELOPMENT_PLAN.md
恢复动作：读哪个报告，然后执行哪个checkbox
```

模板字段在首次执行时填写真实值；未发生事件使用明确的 `none`，缺外部输入使用第4.3节结构，不保留“实际值”文字作为已填写记录。

### 9.2 每个 `WPxx.md` 的固定字段

```markdown
# WPxx 执行报告

关联任务及适用子交付：
代码commit/未提交patch摘要：
实现文件与主要行为：
版本：JDK/vendor/patch、Maven、Go、client、HBase/Hadoop、proto
环境：public-local / target / transport-fixture / native-etcd
profile/schema/salt与数据规模：
命令及起止时间：
每条命令的exit code：
test/case数量、passed/failed/skipped：
实际结果及与预期差异：
原始产物路径与SHA-256：
脱敏证据索引：
输入状态与未覆盖项：
外部评审记录或待评审状态：
下一WP/checkbox：
```

日志很大时报告引用忽略目录中的原始文件及摘要，并提交可复核的脱敏结果；凭据不得因“原始证据”要求而被保留。结果未知、超时与确定失败分别记录。

### 9.3 输入请求模板

```text
正在执行 M0 的 WPxx.target。已有公开工程/协议交付为：报告路径。
缺少输入：Bxx 所需具体事实与测试资源权限。
这个输入影响：具体子步骤与无法证明的验收项。
请提供：认证机制/版本/规则等非敏感说明，以及既有安全配置引用。
收到后执行：具体probe或验证命令。
当前继续：列出无依赖工作包。
```

只在确有输入缺口时使用请求；已有上下文能确定的信息先自行读取。没有回复不能视为输入或授权。

## 10. 来源与执行时复核

以下资料用于核对真实契约；先检查本计划的固定commit和版本，再读对应实现，不能用浮动README替代源码。

- [项目开发计划](../../../DEVELOPMENT_PLAN.md)：M0范围、原任务、阶段和计数的依据。
- [Milvus v2.6.18固定commit的go.mod](https://github.com/milvus-io/milvus/blob/5aee61bff0de2c9650af81f2e9b0bef44efe13fe/go.mod)：Go/client/api/server/grpc版本候选；具体调用由WP01审计。
- [etcd v3.5.23固定commit的rpc.proto](https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/api/etcdserverpb/rpc.proto)：method、字段、import和stream定义。
- [etcd固定commit的rpctypes错误](https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/api/v3rpc/rpctypes/error.go)：status/message和消费者错误映射。
- [gRPC Java generated-code](https://grpc.io/docs/languages/java/generated-code/)：Java package、ImplBase和stream代码生成行为。
- [Maven Wrapper](https://maven.apache.org/tools/wrapper/) 与 [Toolchains](https://maven.apache.org/plugins/maven-toolchains-plugin/usage.html)：固定构建入口及toolchain使用。
- [HBase Java支持矩阵](https://hbase.apache.org/docs/configuration/basic-prerequisites/)：2026-10-10读取时JDK21推荐组合含2.6.8+/2.7；对目标版本仍做真实连接和操作验证。

Goal的生命周期规则依据执行环境实际提供的 `create_goal/get_goal/update_goal` 契约；它们是工具调用，不是上述仓库中的Java/Maven功能，也不是假定存在的命令行子命令。
