# J07：Go 驱动与原生 etcd 对照器

本层提供可重复运行的验证工具。Java 生产服务继续对尚未实现的 RPC 返回 `UNIMPLEMENTED`；本层没有实现 KV、Txn、Watch、Lease 或 token 业务。

## 1 版本和入口

依赖以 `build/dependency-lock.json` 与 `testkit/go/go.sum` 为准：Go 1.27.2，etcd api/client/server v3.5.23，grpc-go v1.79.3。启动器关闭 Go home 配置、自动工具链、workspace、私有模块配置和认证；只使用公开 Go proxy 与校验数据库，禁止直接 VCS 下载。缺少公开依赖时明确失败。

```bash
scripts/dev/go-m0.sh test ./...
scripts/dev/go-m0.sh vet ./...
scripts/verify/native-oracle.sh --scenario testkit/scenarios/m0-native.yaml
scripts/verify/wire.sh --cases testkit/cases/m0-wire.json
scripts/verify/j07.sh
```

`j07.sh` 要求源码已提交，输出本地 `.artifacts/m0/j07/<run>/summary.json`、Go 普通/race 结果、原生 trace、互通 trace 和实际 Java XML。缺失、失败、跳过、空测试或矛盾计数都会失败。执行记录与原始报告不提交到仓库。

## 2 Trace 契约

每个 JSONL 记录包含 case/ordinal、完整 method、请求、响应、错误分类、流终止、证据范围和耗时。bytes 使用 base64，int64/uint64 使用十进制字符串；oneof、字段零值、Header、重复字段的顺序均保留。typed-nil 响应为 `null`，不能替换原始 RPC 错误。

错误记录包含 gRPC code、Go 消费端分类和白名单内的错误身份；例如 `Compacted` 与 `FutureRev` 都是 `OutOfRange`，仍必须区分。原始异常文本与 gRPC metadata 不写入 trace。认证账号、角色、密码、token 等字段只保留存在性或数量。公开验证只使用合成数据；base64 仍保留原始 KV bytes，真实业务 trace 应仅保存在受控本地目录，不作为公开报告上传。通用 Range 驱动只接受 transport/transport-fixture 范围，不能由参数把探测结果标成业务验收。

快照拒绝未知 protobuf wire 字段、循环/过深对象、非法字符串；JSONL 拒绝重复键、孤立 Unicode surrogate、缺失 envelope、尾部数据、缺少换行的截断记录和空输入。预算为单记录 64MiB、总计 256MiB、10000 条，JSON 深度 256、值数量一百万；生成 protobuf 图另限深度 64、原始 bytes/string 总量 32MiB。

## 3 原生场景与对照规则

原生入口启动独立的嵌入式 etcd Go 程序，使用 loopback 监听与本次专属新数据目录；有界等待启动和 RPC，SIGINT/SIGTERM 直接到达程序的取消上下文，结束时关闭自己的 client/server。trace 与 metadata 限定在指定 artifact root 内，以排他创建及受根目录约束的文件操作防止覆盖或路径逃逸。不要在不可信共享目录运行；本层没有验证上游发行二进制或容器镜像。

固定场景覆盖二进制 KV、Limit/More/CountOnly、Txn 成功/失败分支、Delete PrevKV、Lease 绑定/续租/撤销、列表 revision 到 Watch R+1、Compact 与历史压缩错误。两次独立运行的 trace 必须等价。

对照时仅忽略耗时，分别映射 cluster/member/lease ID，并对所有正 revision 应用同一个场景偏移；相对 revision 与请求中的零值保持不同。映射保留引用和事件关系，不能删除或排序响应来凑一致。

native/target 跨范围对照按成功 Range 请求识别两项已接受差异：普通 Limit>0 的 Count 必须为目标本页 len(Kvs)，范围 CountOnly+Limit=1 的 Count 必须为目标真实存在性 0/1。C03/C05 是覆盖场景，不是以标签绕过规则的开关。More、Kvs、Header、revision、错误及其他字段仍严格一致；双方 Count 数值相同也不放过非法目标成功结果。失败观察的 response=null 不套成功 Count 规则，双方相同错误可等价、不同错误身份仍拒绝。native/native 和 target/target 不豁免 Count。未分类错误不能建立等价结论。十种损坏样本分别验证事件、More、deleted、revision、Txn 分支、PrevKV、流终止、同 code 不同错误、存在性及 JSONL 截断均会失败；普通分页另以真实 native trace 派生目标样本核对页内 Count、非法总数及其他字段差异，派生样本不代表目标 HBase 实测。

## 4 验证边界

**表 1 验证证据与适用边界**

| 范围 | 本层证据 | 不代表的结论 |
| --- | --- | --- |
| transport | 固定 Go client/raw stub 到真实 Java 骨架 | Java 业务成功 |
| transport-fixture | 独立错误夹具中的 bytes/int64、错误 sentinel、重试/deadline、流终止和清理 | 后端存储正确 |
| native-etcd | 固定 Go server 模块执行上述原生场景 | 33 项完整场景通过 |
| target-hbase | 本层未获得目标环境证据 | 真实身份、权限、TLS 或生产兼容已验收 |

静态清单核对 41 项公共 API descriptor 方法、7 项额外 Lock/Election 方法与 C01–C33 的关联。额外方法的流形态还与固定 Go server 生成描述核对；`Election/Observe` 是服务端流。完整 Java 业务对照随对应 J 任务交付，Milvus 消费者验收在 M6 执行。
