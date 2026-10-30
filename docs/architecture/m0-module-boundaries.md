# M0 模块与内部 port 边界

J08 基于当前八个 Maven 模块及 J04–J07 的真实使用核对依赖方向。公共扩展与内部装配分别处理，避免把尚未稳定的存储/提交模型冻结为公开接口。

## 1 当前模块与依赖

**表 1 M0 模块、依赖与职责**

| 模块 | 当前直接项目依赖 | 当前职责 |
| --- | --- | --- |
| `protocol` | 无 | 固定上游 proto、生成 Java/gRPC 和 descriptor/wire 守卫；生成类型不手改。 |
| `spi` | 无；生产仅 JDK | 盐策略、后端身份及不可变 DTO。 |
| `core` | `spi` | `RequestScope`、`BackendFailure`；后续内部领域与存储 port 的归属。 |
| `hbase` | `core`、`spi` | 统一身份适配、HBase 工厂、探针及请求资源桥接；HBase 类型只经专属 API 暴露。 |
| `grpc` | `protocol`、`core`、`spi` | 真实注册方法、Header/Error 投影、请求 deadline/取消和有限结果分类。 |
| `admin` | `hbase`、`spi` | 初始化计划、确认/应用和原型 CLI；不承担提交恢复或业务 handler。 |
| `server` | `grpc`、`core`、`admin` | 严格运行配置、监听绑定、进程生命周期和有限指标装配；间接使用 HBase/SPI。 |
| `testkit` | `protocol`、`spi` | 外部扩展契约验证；Go 驱动、原生 etcd 对照与 Java 互通工具。它不进入生产服务依赖。 |

表中是当前直接项目依赖；外部库和 test 依赖仍按各 POM 与锁文件管理。`server` 当前通过 `admin` 复用 J05 配置 DTO/校验，不再定义另一套表组或盐模型。

## 2 内部 port 与扩展边界

- `ReadView`、逻辑提交/发布状态、fencing、恢复水位等未来内部类型归 `core`，不进入 `spi`。本层不为尚无调用方的模型增加接口。
- `grpc.RequestCompletionRecorder` 是内部装配 port，由 `server.RequestMetrics` 实现。其固定 method/outcome、trace ID、scope 与耗时不包含 key、账号、token 或原始后端异常；trace ID 不作为指标标签。
- `HBaseConnectionFactory` 属于 HBase 专属 API，借出的共享连接由工厂持有；请求取消通过 dependent future 和请求资源边界传播。通用 SPI 不出现 `AsyncConnection`、UGI 或 gRPC Context。
- 配置、指标、绑定扩展是后续候选面；本层复用现有 server/admin/grpc 内部类型。J18/J19/J58 再根据真实装配需求确定公开插件加载与发行兼容。

生产 handler 仍以 `UNIMPLEMENTED` 终止。provider fixture、native etcd、编译/启动和协议互通都有各自证据，不能推导完整 KV/Lease/Watch/Auth 或目标集群验收。

边界与当前[公开 SPI 契约](../spi/m0-spi-contract.md)一同由 `scripts/verify/m0-spi.py` 和 `scripts/verify/j08.sh` 核对。
