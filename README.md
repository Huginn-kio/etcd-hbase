# etcd-hbase

An etcd v3 API-compatible server backed by Apache HBase, built with Java 21.

面向开源的独立服务。公开构建与扩展接口按开发计划逐步交付。

## 开发与评审

- [开发计划与进度记录](DEVELOPMENT_PLAN.md)：架构与扩展边界、9 个阶段、78 个主任务及子任务、4 个开发检查点、完成标准、风险与评审记录。
- [Milvus 调用基线](docs/milvus-etcd-usage.md)：以最低支持版本 v3.0.1 固定调用、client/proto 版本与错误消费者。
- [首版 API 与验证矩阵](docs/api-matrix.md)、[分类验收目标](docs/acceptance-targets.md)、[J00 审计报告](docs/reports/j00-baseline-audit.md)：调用范围、目标初稿与静态审计依据。
- [J03 gRPC 骨架与互通](docs/protocol/j03-grpc-skeleton.md)：启动参数、Header/Error 契约、Java/Go 验证与后端版本边界。
- [J04 后端身份与 HBase 探针](docs/hbase/j04-backend-access.md)：身份/连接生命周期、多种认证方式、凭据载体及临时环境验证范围。

- [运行配置与请求生命周期](docs/config/j06-runtime-configuration.md)、[Go 原生对照工具](docs/testing/j07-protocol-oracle.md)、[SPI 契约](docs/spi/m0-spi-contract.md)。
- [公开构建与运行包](docs/build/j09-public-delivery.md)：两次完整构建、真实运行包/镜像、证据索引与未决发行输入。

当前提供可启动的协议骨架，未实现方法明确返回 UNIMPLEMENTED。完整存储和业务语义按后续 J 任务及阶段推进；首版 Count 与其他兼容范围以开发计划的能力矩阵为准。

部署目标为无本地业务持久化的 K8s 多副本；跨副本认证、提交接管和 Watch/KeepAlive 恢复，以及 HBase 原生多版本存储评估已纳入计划，均待实现与验证。
