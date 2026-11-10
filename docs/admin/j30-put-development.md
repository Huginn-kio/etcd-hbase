# J30 本地 Put 开发入口

J30 的本地入口在真实 HBase 上验证 Put 和恢复。它只激活显式的 loopback、无认证开发 profile；目标环境的认证、TLS 和跨进程 owner 转发尚未接入该入口。Range、DeleteRange、Txn、Lease 等尚未交付的 RPC 仍返回 `UNIMPLEMENTED`。该入口不代表 M2/M3 阶段或目标环境已验收。

## 1. 初始化新表组

准备独立的本地 HBase 和空 namespace，按实际端口调整 `config/examples/m3/initialize.json` 与 `config/examples/m3/server.json` 中相同的 HBase profile。两份配置的表组、盐和布局必须一致；示例中的桶数和布局仅用于开发。选择固定的 pin 配置，下面使用 `2:16:60000`，与示例 activation 文件一致。

```bash
scripts/dev/m3-admin.sh init plan \
  --config config/examples/m3/initialize.json \
  --table-group local --pin-registry 2:16:60000
```

检查表、布局和 pin 配置后，将输出中的 `confirmation_fingerprint` 原样用于创建：

```bash
scripts/dev/m3-admin.sh init apply \
  --config config/examples/m3/initialize.json \
  --confirm-table-group local --confirm-plan 'sha256:<plan-fingerprint>' \
  --pin-registry 2:16:60000
```

指纹同时覆盖表组方案和 pin 参数。初始化必须由一个操作者独占执行，并排除其他初始化和直接写入。它不会接管已有表；失败或结果未知时先检查实际表和元数据，不能直接重试或自动删除。已有 M0/M2 表组需要单独制定和验证迁移方案。

## 2. 启动本地服务

`config/examples/m3/activation.json` 显式设置实例 member ID、owner 租约、超时、资源预算和 pin 参数。每个实例使用稳定且唯一的非零 uint64 member ID；同表组的 pin 参数须与初始化时一致。JSON schema 可检查基本字段范围，运行时还会校验 uint64 上界、心跳与租约关系及跨配置预算。
示例监听 `127.0.0.1:23790`，可按本地环境调整。

```bash
scripts/dev/runtime-server.sh --config config/examples/m3/server.json \
  --m3-config config/examples/m3/activation.json
```

表组的逻辑 cluster ID 在初始化时持久化，重新启动后读取原值；响应的 member ID 来自实例配置，当前 `raft_term` 为 `0`。数据就绪依赖表绑定、恢复与 owner 检查成功；监听器启动本身不表示数据已就绪。此版本仅支持同进程 owner 调用，遇到其他实例持有 owner 时不会伪造跨进程转发成功。
启动校验会读取 HBase 表描述与实际 backend cluster ID；目标环境所需的权限尚未验证。

`RuntimeServer.metrics()` 按监听器暴露请求计数与 Put 的排队、准备、发布及收尾、结果读取、响应阶段耗时，以及 `onNext` 接受的 protobuf 响应字节数。同一表组的监听器共享指标；结果不包含 gRPC 帧或网络字节，也不代表目标环境吞吐量。
