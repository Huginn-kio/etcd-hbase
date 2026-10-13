# 贡献指南

本项目实现面向 Milvus etcd v3 gRPC 调用路径的独立 Java 服务。提交前先确认变更对应的开发任务、协议边界和验收证据；当前阶段与未决输入见 [`DEVELOPMENT_PLAN.md`](DEVELOPMENT_PLAN.md)。

## 开发环境

- 使用锁定的 Eclipse Temurin JDK 21.0.12.1+1 和 Maven Wrapper。版本、归档摘要与公开来源见 [`docs/build/m0-toolchain.md`](docs/build/m0-toolchain.md) 和 [`build/dependency-lock.json`](build/dependency-lock.json)。
- 将 `ETCD_HBASE_JDK21_HOME` 指向该 JDK 的 home 目录，然后运行 `scripts/dev/mvn21.sh clean verify`。
- 公开构建只需访问 Maven Central 和公开 JDK 下载源；不要求私有 Maven 仓库、扩展仓库或目标环境凭据。
- 不要提交生成的 toolchain 文件、构建输出、环境配置或任何凭据。目标环境参数通过受控的外部输入流程提供，不放入公开样例。

## 变更与 Pull Request

1. 描述对应的 Jxx/WP 和行为边界；只纳入该变更所需的文件。
2. PR 以仓库 `main` 为目标分支。依赖尚未合并的 PR 时，也以 `main` 为 base，并在描述中链接依赖 PR 和说明依赖内容。
3. 每项验证写明命令、运行环境和结果。区分公开构建、mock/fixture、真实 HBase 以及目标环境证据；没有运行的验证要标明原因。
4. 编译成功只证明构建范围内的源代码可编译，不能代替协议互通、真实 HBase 行为或 M0 阶段验收。
5. 若改动依赖、proto 来源或生成器，同步更新版本、来源、摘要及第三方许可信息，并说明是否影响已有数据格式或公开 API。
6. 日志和 PR 附件中不得包含密码、token、密钥、完整业务 key 或未经脱敏的目标环境数据。

## Review 关注点

- 正确性和错误边界；不可用未实现的业务成功响应伪装通过。
- 模块依赖方向；`spi` 不依赖 HBase、gRPC 实现或 `core` 内部类型。
- 可读性、资源关闭、deadline/cancel 传播与配置校验。
- 安全日志、依赖来源、开源许可和性能预算。
- 验证是否覆盖所声明的验收层次，是否把缺失的目标输入明确标出。
