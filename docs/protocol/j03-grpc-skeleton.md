# J03：gRPC 服务骨架与最小互通

J03 接续已合并的 J01 工程和 J02 协议基线，注册 KV、Watch、Lease、Auth、Cluster、Maintenance 六项服务，共 41 个协议方法。生产 handler 对尚未实现的功能返回 `UNIMPLEMENTED`，不返回 KV、`token`、`revision` 或成功 Header。

## 1 启动

构建、测试和运行使用 `build/dependency-lock.json` 中锁定的 Java 21。设置 `ETCD_HBASE_JDK21_HOME` 后运行：

~~~bash
scripts/dev/j03-server.sh --bind-host 127.0.0.1 --port 2379 --table-group local-skeleton
~~~

入口使用打包后的模块 JAR 和运行依赖；可指定端口 `0`，由日志报告实际端口。当前只接受 `127.0.0.1`、`::1`、`localhost`，`--table-group` 是显式的逻辑绑定标识，启动不确认 HBase 表组存在，也不建立后端连接。日志中的 `data_readiness` 为 `UNIMPLEMENTED`。

`SIGTERM` 触发有界关闭。J03 只提供三个启动参数；YAML、环境覆盖、表 schema/盐校验与完整运行预算由 J05/J06 接续。

## 2 Header 与错误契约

`ResponseHeaderFactory` 接收显式 `cluster`、`member`、`revision`、`term` 参数并保留协议位模式；骨架 handler 不调用它伪造状态。`BackendFailure` 位于 `core`，只包含有限失败类别、安全描述与内部 `cause`，不依赖 HBase 或 gRPC。

`ErrorProjection` 在 `grpc` 层映射认证、权限、不可用、超时、取消、资源限制和未知错误。认证失败保持 etcd 的 `InvalidArgument`/canonical message，权限不足使用 `PermissionDenied`；原始异常文本及 `cause` 不进入 gRPC 错误。`Watch`/`KeepAlive` 在收到请求或空 half-close 后明确终止；客户端取消不生成成功结束或新的业务响应。

## 3 验证

~~~bash
scripts/verify/j03.sh
~~~

该入口核对锁定的 Go 工具链与模块、Go checksum/格式/`vet`、proto 来源和 Java 测试，并显式启用 `server` 的 `j03-wire` 集成测试 profile。缺少 `GoInteropIT` 报告、零测试、失败或 skipped tests 都会导致失败。

Java 测试通过真实端口调用全部 41 个方法，并检查 J03 handler 的明确拒绝描述，避免把“服务没有注册”也当成 `UNIMPLEMENTED` 验证通过。测试还覆盖端口占用、非法绑定、参数、重复关闭、stream half-close 和客户端错误。

`testkit/j03-go` 是固定 etcd v3.5.23 client 的最小 smoke，验证原始字节、超过 `2^53` 的 `int64`、`clientv3`/`apiv3`、`Watch`/`KeepAlive` 终止、认证/权限错误识别和 deadline 传播。错误 fixture 仅存在于 `server` 测试源，不访问 HBase，也不返回成功 KV/Header/`token`。报告保留在 `.artifacts/m0/j03/`；通用 trace/replay/native-etcd 对照工具由 J07 实现。

默认 Maven `verify` 运行 Java 单元测试，Go 互通通过显式 `j03-wire` profile 运行；J03 CI 必须调用完整的 `scripts/verify/j03.sh`。

## 4 后端版本边界

Java 21 是本服务的编译和运行基线，不要求 HBase 服务端使用 Java 21。项目锁定默认 client 依赖以保证可重复构建；目标 HBase 版本是兼容验证信息，不要求集群与 client 使用同一具体版本。J04 根据可用测试环境验证连接、身份、基础操作及支持范围，未验证的组合保留未验证状态。

参考：[gRPC Java 生成代码与流方法](https://grpc.io/docs/languages/java/generated-code/)、[HBase Java 支持矩阵](https://hbase.apache.org/docs/configuration/basic-prerequisites/)。
