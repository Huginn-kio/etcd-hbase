# 变更记录

本文件记录项目可见的变更。尚未发布的工作保留在 `Unreleased`，不代表任何发布制品已经交付。

## Unreleased

### Added

- 建立 Java 21、Maven Wrapper 和八模块 Maven reactor 的公开工程基线。
- 固定公开依赖候选及来源摘要，并记录 HBase 目标版本仍等待 B02。
- 增加贡献、Pull Request 验证和第三方许可跟踪规则。

### J06–J09 公开工程交付

- 统一运行配置、请求 deadline/取消与资源清理，提供有限标签的基础观测。
- 增加无损 Go trace、原生 etcd 场景与损坏样本对照，明确 transport/fixture/目标业务证据边界。
- 核对盐/身份 SPI 所有权和 JDK-only 依赖，增加外部契约测试。
- 提供两次完整构建的 JAR 摘要对照、真实运行包和 Java 21 非 root 镜像验证；许可与发布仍待 I07/I08。生产业务 RPC 保持 UNIMPLEMENTED。
