# 项目工作约定

- 以 `DEVELOPMENT_PLAN.md` 和 `docs/specs/` 已批准的设计为实现依据，按 J00、J01 等任务顺序交付。M0 的业务 handler 未实现时必须返回 `UNIMPLEMENTED`，不得伪造 KV、revision、token 或成功结果。
- 编写 feature、review 修正、测试、脚本、构建与 CI 使用同一质量标准。按当前会话适用的 DEVELOPMENT.md、REVIEW.md、REFACTOR.md 正文要求读取和执行；编码前、期间和提交前核对正确性、架构、可读性、安全、性能，以及状态所有权、并发、异常、取消和清理。构建或 CI 通过不能替代自审。
- GitHub PR 说明、评论、review 和回复统一使用简体中文；代码标识、协议名、命令和 conventional commit 前缀保留原文。
- 开发分支使用 `codex/` 前缀。推送前 fetch 并 rebase 最新上游 `main`，PR 提向上游 `main`，保留 fork 为 head。存在前置 PR 时明确依赖、有效 base/head 比较链接及 main diff 的累计范围。
- 公共 SPI 只依赖 JDK；HBase 专属类型留在 `hbase.api`，内部提交、可见性和 fencing 模型不进入公共 SPI。没有实际装配需求时不新增接口。
- 区分作者自审、独立复核、维护者正式 review、公开临时环境和真实目标验收。缺输入或未运行的检查如实保留，不能以跳过测试制造通过。
- 凭据、真实业务数据、个人机器路径和组织内部信息不进入源码、公开样例或 PR。执行恢复记录与原始报告保留本地；代码、产品说明和主计划状态可随所属任务提交。
- 除非用户另行授权，PR 保持开放供审阅，不批准、合并、发布制品或部署服务。
