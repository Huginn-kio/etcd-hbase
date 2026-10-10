# M0 执行记录

Goal objective：完成 etcd-hbase M0（J00–J09 / WP00–WP23）的实现、分步验证和可评审交付；目标 HBase 身份及基础操作必须有真实证据，缺少输入时不宣称 M0 验收通过。

Goal ID：01a123e5-d309-7540-b6c2-2f33a3b0a731（active；未设置 token budget）
执行工作树：/Users/shikang.chen/.codex/worktrees/m0-goal-plan/etcd-hbase
开发分支与基线 commit：codex/m0-engineering；56bef2b1248d970cd2fe41f36249d96610913ae4
计划 SHA-256：5746e412718dd8a6f81f6f0b0fe1e0bd97b929c1469f140e47a9b29a21018929
本轮 run-id：20261010T070004Z-56bef2b
当前 WP 及 checkbox：WP00、WP01、WP05 完成；WP02/WP03/WP04 进行中
下一条可执行命令：执行 WP02，锁定公开 Java 21/Maven/protobuf 与最小工程依赖；目标 HBase 组合继续等待 B02
已验证 WP 与证据：WP00、WP01、WP05，见 docs/reports/m0/20261010T070004Z-56bef2b/WP00.md、WP01.md、WP05.md
等待输入：B01/B02/B03/B04/B05/B06/I07/I08，详见 docs/inputs/m0-inputs.yaml；不阻塞无依赖 public 工作
跨阶段残留：按 M0 计划记录 J18/J19/J27/J50/J51/J52/J55/J56/J58/J59/J84/J85 等衔接项
最近失败：PR #1 的 GitHub reviewer request 被拒绝（HTTP 422：`reidchan` 不是该仓库 collaborator）；PR 已开放，但目前没有 reviewer request。
主任务状态来源：DEVELOPMENT_PLAN.md
恢复动作：先读取本文件、当前 run 的 WP 报告及 goal 状态，再执行最小未完成 WP checkbox

## 执行约定

- M0 只有一个 goal；WP00–WP23 是该 goal 的步骤，不创建子 goal。
- PR 栈共 10 层，栈底分支为 codex/m0-engineering，目标基线为 docs/development-plan；每层通过 PR 正文链接前一层和本层 WP。
- 每个可评审 PR 请求 GitHub reviewer reidchan。评审意见在所属 PR 层修复并复验；未经授权不合并。
- PR #1：`https://github.com/Huginn-kio/etcd-hbase/pull/1`，基于 `docs/development-plan`，状态 OPEN；尝试请求 `reidchan` 后 GitHub API 返回 HTTP 422，原因是只有 repository collaborator 可被请求评审。账号已核实为 `reidchan`，不擅自更改仓库访问权限；review request 当前未建立，需仓库所有者授予合适的 collaborator 权限后重试。
- 后续 PR 标题使用 `[n/10]` 并逐层基于前一层；每层提交 WP、验证结果、stack dependency 和 reviewer 请求状态。
- public HBase 证据不能替代目标 HBase 身份与操作证据；transport fixture 不能替代真实后端失败验证。
- 仅在实现、适用验证和输入事实有证据后更新 DEVELOPMENT_PLAN.md；没有目标身份输入时不得勾选 J04 或 M0 阶段验收。
- 已锁定公开 Milvus 基线 `pkg/v2.6.18` / `5aee61bff0de2c9650af81f2e9b0bef44efe13fe`，并完成 WP01 静态调用审计；不存在等价/互通运行结果。
- WP05 的 11 项生产目标都维持 `missing-input`，工程 profile 独立为 `m0-public-smoke-v1`；目标数值不由 smoke 或本机容量推定。

## 当前环境快照

- 默认 java/javac：Oracle Java 8u421；Maven 3.9.8 当前也通过 Java 8 启动。
- 可用 Java 21：Oracle JDK 21.0.4，路径 /Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home；尚未作为发行工具链决定。
- Go：go1.26.7；protoc：libprotoc 36.2；Docker：未安装或不可用。
- 主工作区 /Users/shikang.chen/IdeaProjects/etcd-hbase 有预存的未跟踪 .idea/，不纳入本执行分支。
