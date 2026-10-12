# M0 执行进度

## 基线与门槛

- 需求基线：`DEVELOPMENT_PLAN.md` v0.6，内容对应 commit `56bef2b1248d970cd2fe41f36249d96610913ae4`；文件 SHA-256 为 `218794ac22f2c63e9a57cfd77a10ee75f5ab554a2d09aa9dff423acb1d7996e5`。
- 初始实施基线：上游 `main` commit `f0b05ae30066d1bc14bd271142ee2e4996f93a69`，包含已批准并合并的 M0 设计 PR #2。
- PR 栈最近重基基线：上游 `main` commit `b212ee4719fa0533594c561de9df15faa6b5cb8e`（2026-10-12）。上游没有 `master` 分支。
- WP01 的 Milvus v3.0.1 调用、API 和验收目标基线已在当前上游 `main` 的 J00 交付中；本执行栈不重复提交旧版调用审计。
- 目标 HBase 版本、身份机制和真实操作证据仍待 B02、B03、B06；公开 smoke 或 no-auth 结果不能替代目标环境证据。

## PR 栈与工作包

| 层 | 工作包 | 分支 | 状态 |
|---:|---|---|---|
| 1/10 | WP00、WP01、WP05 | `codex/m0-01-foundation` | PR #4 open，已重基到当前 `main`；WP01 由上游 J00 基线满足 |
| 2/10 | WP02–WP04 | `codex/m0-02-toolchain` | PR #5 open，已重基到第 1 层 |
| 3/10 | WP09–WP10 | `codex/m0-03-protocol` | PR #6 open，已重基到第 2 层 |
| 4/10 | WP06–WP08、WP13 | `codex/m0-04-spi-config` | 本地实施中；SPI 和盐候选已验证 |
| 5/10 | WP11、WP12、WP20 | `codex/m0-05-testkit-matrix` | 未开始 |
| 6/10 | WP14–WP15 | `codex/m0-06-request-identity` | 未开始 |
| 7/10 | WP16–WP17 | `codex/m0-07-hbase-admin` | 未开始 |
| 8/10 | WP18–WP19 | `codex/m0-08-grpc-skeleton` | 未开始 |
| 9/10 | WP21–WP22 | `codex/m0-09-interop-ci` | 未开始 |
| 10/10 | WP23 | `codex/m0-10-delivery-evidence` | 未开始 |

所有 PR 都以原仓库 `main` 为目标分支；创建或更新 PR 前，先将对应分支重基到最新上游 `main`。从第 2 层起，head 分支继承前一层并在描述中链接依赖 PR；前序 PR 尚未合并时，后续 PR diff 会包含前序层提交。按层次顺序评审，前序层合并后再更新后续 head，使 diff 收敛到未合并层。

## 当前进度

- 已完成并提交 PR：WP00/WP05（PR #4；WP01 证据来自其当前上游基线）、WP02–WP04（PR #5）、WP09–WP10（PR #6）。三条 PR 均请求仓库可请求 reviewer `Reidddddd`；GitHub 不允许指定的账号 `reidchan`，状态以 PR 页面为准。
- 第 4 层当前分支：`codex/m0-04-spi-config`，基于重基后的 PR #6 head。WP06 身份 SPI、WP07 CRC32 盐策略候选已实现；Java 21 下 `spi` 模块 6/6 项测试通过。
- 当前工作包：WP08 候选表配置与 WP13 运行配置装载/校验；目标盐和表配置规则仍等待 B01/B05，不会由公开候选替代。
- 已验证报告：`docs/reports/m0/20261012T061144Z-f0b05ae/WP00.md`、`WP05.md`，协议报告见 `docs/reports/m0/20261012T080404Z-f0b05ae/`。
- 未决输入：B01–B06、I07、I08；具体事实、责任角色和解除动作见 `docs/inputs/m0-inputs.yaml`。
- 最近失败：PR 分支重基时遇到 Milvus 基线文件冲突；已保留最新上游 J00 基线并移除重复的旧版审计材料。

## 恢复步骤

先检查本文件、当前层的 WP 报告、`DEVELOPMENT_PLAN.md` 和当前分支状态；从依赖已满足的最小未完成 WP 继续。目标身份或目标 HBase 输入缺失时，只标记相应 target 子步骤待输入，不据此宣称 M0 验收完成。
