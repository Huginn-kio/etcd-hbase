# M0 etcd 协议源码与生成

M0 协议基线固定为 etcd v3.5.23。`protocol/proto-sources.lock.json` 记录上游仓库、commit、文件路径和 SHA-256；锁定的源文件原样保存在 `protocol/src/main/proto/`。其中 `rpc.proto` 的服务、消息和字段定义来自 etcd，`kv.proto` 与 `auth.proto` 是它的消息依赖。gogo 和 Google API proto 只用于解析上游导入的扩展选项及 HTTP 注解。

Maven `protocol` 模块使用锁文件中的 protoc 4.36.2 和 protoc-gen-grpc-java 1.84.1 生成 protobuf 消息类与 gRPC Java 服务绑定。生成结果只写入 `target/generated-sources/`，不提交、不手工修改。协议源升级必须同时更新来源、commit、文件摘要、生成器和 descriptor 契约，并重新检查 Go 客户端兼容行为。

运行 `python3 scripts/verify/m0-protocol.py` 校验 proto 文件清单、逐文件 SHA-256 和生成器版本。`EtcdProtocolDescriptorTest` 守卫服务与方法集合、Watch/KeepAlive/Snapshot 的流方向、关键字段编号和类型，以及 KV bytes/int64 的 protobuf round-trip。该测试证明生成契约与固定源一致；它不表示这些 RPC 的业务语义已经实现。尚未实现的 handler 仍须返回 `UNIMPLEMENTED`。

第三方源文件的许可和链接记录在 `THIRD_PARTY_NOTICES.md`；项目正式许可证、完整传递依赖通知和发行物 SBOM 仍等待 I07 及发行前审查。
