# M0 执行进度

## 基线与门槛

- 需求基线：`DEVELOPMENT_PLAN.md` v0.6，内容对应 commit `56bef2b1248d970cd2fe41f36249d96610913ae4`；文件 SHA-256 为 `218794ac22f2c63e9a57cfd77a10ee75f5ab554a2d09aa9dff423acb1d7996e5`。
- 实施基线：上游 `main` 的 commit `f0b05ae30066d1bc14bd271142ee2e4996f93a69`，包含已批准并合并的 M0 设计 PR #2。
- PR #2 的维护者评审已批准设计并同意进入 M0 实现；M0 阶段验收仍未通过。
- 目标 HBase 版本、身份机制及真实操作证据仍待 B02、B03、B06；公开 smoke 或 no-auth 结果不能替代目标环境证据。

## PR 栈与工作包

| 层 | 工作包 | 分支 | 状态 |
|---:|---|---|---|
| 1/10 | WP00、WP01、WP05 | `codex/m0-01-foundation` | PR #4 open，评审待回复 |
| 2/10 | WP02–WP04 | `codex/m0-02-toolchain` | PR #5 open，验证完成、评审待回复 |
| 3/10 | WP09–WP10 | `codex/m0-03-protocol` | 本地进行中 |
| 4/10 | WP06–WP08、WP13 | `codex/m0-04-spi-config` | 未开始 |
| 5/10 | WP11、WP12、WP20 | `codex/m0-05-testkit-matrix` | 未开始 |
| 6/10 | WP14–WP15 | `codex/m0-06-request-identity` | 未开始 |
| 7/10 | WP16–WP17 | `codex/m0-07-hbase-admin` | 未开始 |
| 8/10 | WP18–WP19 | `codex/m0-08-grpc-skeleton` | 未开始 |
| 9/10 | WP21–WP22 | `codex/m0-09-interop-ci` | 未开始 |
| 10/10 | WP23 | `codex/m0-10-delivery-evidence` | 未开始 |

所有 PR 都以原仓库 `main` 为目标分支；从第 2 层起，head 分支继承前一层并在描述中链接依赖 PR，按顺序评审。前序 PR 尚未合并时，后续 PR diff 会包含其变更；前序合并后再更新后续 head，使差异收敛到未合并层。每层记录 WP 范围和验证结果，外部评审状态以 GitHub 实际结果为准。

## 当前进度

- 当前层：3/10。WP00、WP01、WP05 已完成并提交 PR #4；WP02–WP04 已完成本地验证并提交 PR #5，PR #5 head 接续第 1 层分支，评审仍待回复。
- 已验证报告：`docs/reports/m0/20261012T061144Z-f0b05ae/WP00.md`、`WP01.md`、`WP05.md`。
- 当前工作包：WP09–WP10，固定 etcd v3.5.23 proto 来源与 Java 生成配置，并建立 descriptor/wire 守卫。目标 HBase 组合仍等待 B02；评审意见到达后在对应层处理并更新验证记录。
- 未决输入：B01–B06、I07、I08；具体事实、责任角色和解除动作见 `docs/inputs/m0-inputs.yaml`。
- 最近失败：none。

## 恢复步骤

先检查本文件、当前层的 WP 报告、`DEVELOPMENT_PLAN.md` 和当前分支状态；从依赖已满足的最小未完成 WP 继续。目标身份或目标 HBase 输入缺失时，只标记相应 target 子步骤待输入，不据此宣称 M0 验收完成。
