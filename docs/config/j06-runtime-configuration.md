# J06 运行配置与请求生命周期

统一运行配置使用严格 UTF-8 JSON：[开发示例](../../config/examples/j06/server.json)、[schema](../../config/schema/server.schema.json)。J05 初始化 CLI 和运行配置共用后端、盐、表配置模型及严格读取器；运行配置额外声明 listeners 和 resources。未知/重复字段、缺失或 null 必填项、字符串到数值的隐式转换、数字枚举、尾随 JSON 和超过 65,536 字节的输入均失败。schema 测试递归核对 DTO 字段、required、基础类型和枚举；表组引用、物理 scope、权限意图及跨字段约束由共享校验器验证。

加载顺序为文件、有限环境覆盖、完整校验。环境覆盖包括 `ETCD_HBASE_LISTENER_BIND_HOST`、`ETCD_HBASE_LISTENER_PORT`、`ETCD_HBASE_LISTENER_TABLE_GROUP_ID`（第一项 listener），以及资源字段对应的 `ETCD_HBASE_MAX_CONFIG_BYTES`、`ETCD_HBASE_MAX_CONCURRENT_REQUESTS`、`ETCD_HBASE_REQUEST_DEADLINE_MS`、`ETCD_HBASE_MAX_RPC_BYTES`、`ETCD_HBASE_MAX_REQUEST_BYTES`、`ETCD_HBASE_MAX_SCANNERS_PER_TABLE`、`ETCD_HBASE_MAX_QUEUED_REQUESTS`。覆盖后的值仍受同样验证；原始文件与覆盖项字节合计受配置预算约束。未知 `ETCD_HBASE_` 运行变量失败；已知工具链变量 `ETCD_HBASE_JDK21_HOME`、`ETCD_HBASE_MAVEN_REPOSITORY` 单独忽略。输入流由调用者关闭，错误不回显值或解析器原始异常。

一个监听端点绑定一个表组。端口、host 和 profile 均不接受通配符；主机名不执行 DNS 校验。固定端口的等价 IPv4/IPv6、大小写及尾点主机名、保守 loopback 别名不能重复绑定。端口 0 只允许 loopback。同一物理 quorum/port/namespace 下，换 profile ID 不能绕过表名冲突检查。DEV_NO_AUTH 必须显式选择，listener 和全部 quorum 地址均须 loopback。其他身份只接受 env:// 或 secret:// 引用，不解析或记录凭据值。

```sh
scripts/verify/j06.sh
scripts/dev/runtime-server.sh --config config/examples/j06/server.json
```

配置启动实际绑定多个 gRPC listener，启动中途失败时回收已绑定端口；J03 三参数入口继续可用。当前运行骨架只装配显式本地开发模式，目标身份激活、TLS、Authenticate/token 会话完整装配归 J50/J56。安全 profile 可以离线校验，但未装配时启动明确失败。J04 的认证扩展不因这个运行原型缩减。所有生产业务 handler 继续返回 UNIMPLEMENTED，不创建表、KV、revision 或 token。

每个 listener 独立限制并发请求数、单条入站 RPC 字节及一次调用累计 protobuf 请求字节。没有排队实现，因此 `max_queued_requests` 必须为 0；超额立即返回 RESOURCE_EXHAUSTED。scanner 上限在当前配置中预留给存储 adapter，尚无生产业务 scanner 路径，不能将此字段视为生产表级配额已实现。排队、提交等待、连接池等业务指标随实际 owner 实现，不填虚假测量值。

RequestScope 使用 monotonic deadline；gRPC 采用客户端和服务端预算中较早的 deadline，超时会终止调用。取消、异常和正常完成首先停止接收新工作；有界清理执行器接管释放工作，scope 和 gRPC context 清理均完成后才归还一次准入额度并记录 cleanup 结果。资源清理或 future 的同步完成回调不在共享 deadline 线程执行；取消后取得的资源立即关闭。清理失败按数量记录，继续清理其他资源；致命 Error 在清理后传播。观测失败单独计数，不能改变业务结果或泄漏额度。handler 异常在协议边界映射为安全状态，原始异常/metadata/请求内容不进入请求日志。

J04 HBaseRequestContext 只取消请求持有的 future 和 scanner；借用连接时使用独立等待 future，不能关闭或取消 factory 的共享连接。HBase 已接受的 mutation 不因 future 取消而承诺回滚；已发布提交的恢复归 J27。清理任务数受仍持有的请求额度约束，清理 worker 有上限；进程关闭有界等待清理 drain，超时明确失败，保留已接管的清理任务。资源关闭钩子仍须及时返回。

日志只包含有限方法类别、结果类别、时延、剩余预算和服务端生成 trace UUID。指标只使用方法/结果枚举，记录完成数、累计时延和 cleanup 失败数；准入中请求数从同一 semaphore 状态读取。key、账号、凭据、token、任意异常消息或表组名不作为标签。

验证覆盖实际 TCP gRPC 的取消、server deadline、准入拒绝、流累计字节上限、handler/观测失败，确定性清理竞态，以及真实临时 HBase 的 scanner close 和取消后共享连接读回。公开环境验证不替代目标认证、权限、TLS、B01 盐规则、许可证或制品发布决定。
