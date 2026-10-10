# Milvus etcd 调用基线

## 固定来源

- Tag：`pkg/v2.6.18`
- Commit：`5aee61bff0de2c9650af81f2e9b0bef44efe13fe`
- Go directive：`1.25.8`；本次静态审计机：`go1.26.7 darwin/arm64`
- MVS 结果：`go.etcd.io/etcd/api/v3 v3.5.23`、`go.etcd.io/etcd/client/v3 v3.5.23`、`go.etcd.io/etcd/server/v3 v3.5.23`、`google.golang.org/grpc v1.79.3`
- `go.mod` SHA-256：`b63ede102fc363d3618432f2da8d6b5fcc1f5b3d34b55a938cbd297050a6ec6d`
- `go.sum` SHA-256：`1484b4c6fc5eda2d0bb80ab4c1ba0673441c97725476397c73da921170544a3d`
- 本地 replace：`github.com/milvus-io/milvus/client/v2 => ./client`；审计的 etcd 与 grpc 模块没有 replace。
- Native etcd 对照候选：v3.5.23，commit `1cf4bc40111fd95d1e14a90a09a146357b15eb13`。仅为 disposable 测试对照，认证、部署拓扑和容量须独立决定。

## Usage 清单

下表所有条目都继承本页固定的 Milvus commit `5aee61bff0de2c9650af81f2e9b0bef44efe13fe`。路径行号来自该 commit；`clientv3` 是直接 RPC，`MetaKv`/`sessionutil` 是仓库内包装接口。case ID 是 WP11/WP12 后续驱动应复用的稳定名称前缀，不代表本阶段已经运行互通测试。

| usage_id / case_id | 来源与符号 | 方法、key/range 与选项 | 实际消费字段 | 错误、重试与边界 |
|---|---|---|---|---|
| U01 / `M0-RANGE-PREFIX` | `internal/kv/etcd/etcd_kv.go:80` `WalkWithPrefix` | `Get(rootPath+prefix)`；升序 key 排序、`WithLimit(pageSize)`、`WithRange(GetPrefixRangeEnd(prefix))`；按末键追加 `0x00` 翻页 | `Kvs[].Key/Value`、`More`；逐项调用 callback | RPC 与 callback 错误直接返回；分页调用逐页加 request timeout。完整范围读逐页发生，不是一次原子快照。
| U02 / `M0-GET-PREFIX` | 同文件 `LoadWithPrefix`、`LoadBytesWithPrefix`、`LoadBytesWithPrefix2`、`LoadBytesWithRevision` | `Get(rootPath+key, WithPrefix, SortByKey/Ascend)`；这些全量 prefix API 不带 limit | key/value；`LoadBytesWithPrefix2` 另消费每个 KV 的 `Version`；revision 变体消费 `Header.Revision` | 超时或 RPC 错误返回；响应全量装入切片。消费端需验证大范围内存及 Count 替代语义。
| U03 / `M0-HAS-COUNT` | 同文件 `Has`、`HasPrefix` | 精确 key `WithCountOnly`；prefix 同时 `WithPrefix, WithLimit(1), WithCountOnly` | `GetResponse.Count != 0` | 错误返回 `(false, err)`；`HasPrefix` 明确是存在性查询，不要求 count 的准确总数。
| U04 / `M0-TXN-CAS` | 同文件 `CompareVersionAndSwap*`、`MultiSaveAndRemove*`、`getTxnWithCmp`、`executeTxn` | `Txn.If(compare...).Then(OpPut/OpDelete).Commit()`；prefix 删除使用 `WithPrefix` | `TxnResponse.Succeeded`；有 Range transaction response 时统计 KV；Put 字节统计 | RPC 错误返回；CAS 比较未成功作为 `false`；带 predicates 的 batch 返回事务失败错误。抽象层无统一 retry。
| U05 / `M0-WATCH-REVISION` | 同文件 `Watch`、`WatchWithPrefix`、`WatchWithRevision` | exact key 或 prefix；创建通知；revision 变体另加 `WithPrevKV, WithRev` | watch channel 交给调用方 | 该 wrapper 以 `context.Background()` 建 watch，而非传入参数 ctx；调用方关闭需核对 watcher 生命周期。此处直接调用端还需单独审计。
| U06 / `M0-SESSION-LEASE` | `internal/util/sessionutil/session_util.go:486-620, 673-755` `registerService`、`getOpsForCoordinator`、`processKeepAliveResponse`、`GetSessions`、`GoingStop` | `Grant(sessionTTL)`；租约 `OpPut(...WithLease)`；Compare Version/Value 后 Txn；KeepAlive 流与单独 `TimeToLive` 检查；session Get prefix+key 升序；stop 检查用精确 `WithCountOnly` | Lease ID；`TimeToLive.TTL`；Txn `Succeeded` 和 `Header.Revision`；session `Kvs[].Key/Value` 和 Header revision；KeepAlive stream 消息本身只被 drain，不读取其字段 | 注册按 `sessionRetryTimes` 重试，版本冲突类型排除重试；KeepAlive 流失败后带指数退避重建；租约缺失/本地过期会退出进程；Get/TXN/JSON 错误记录并返回或按循环路径处理。`common.session.ttl` 默认项定义于 `pkg/util/paramtable/component_param.go:944-951`，默认值常量 15s 位于同文件。
| U07 / `M0-SESSION-WATCH-REPLAY` | `internal/util/sessionutil/session_util.go:673-725, 835-955` `GetSessions*`、`WatchServices*`、`handleWatchResponse`、`handleReWatch`；`internal/querycoordv2/server.go:460-480, 629-699` | Get role prefix、按 key 升序，保存读到的 Header revision；Watch role prefix + `WithPrevKV` + `WithRev(revision)`；重建时重读列表 | 初始 session 的 Key/Value/Header revision；PUT 的 `Kv`；DELETE 的 `PrevKv`；状态/ServerID/Address/version | RPC、解码和回调错误传给调用者/重建路径；watch compaction 触发重新列举并以返回 revision 恢复。watch 缓冲为 100；目标兼容必须覆盖回放、删改、compact。
| U08 / `M0-PROXY-WATCH` | `internal/util/proxyutil/proxy_watcher.go:88-218` `WatchProxy`、`getSessionsOnEtcd`、事件处理 | 先取 proxy role prefix 升序列表；Watch `WithPrefix, WithCreatedNotify, WithPrevKV, WithRev(rev+1)` | Get `Kvs[].Value` 与 Header revision；PUT `Kv.Value`；DELETE `PrevKv.Value` | Get/JSON 错误返回；compact 时重建 Watch；重建失败及其他 watch error 当前 fail-fast/panic；单个事件解析/回调失败记录日志。
| U09 / `M0-DISCOVERY-WATCH` | `internal/util/streamingutil/service/discoverer/session_discoverer.go:84-190` `Discover`、`initDiscover`、`watch`、`handleETCDEvent`；`internal/util/streamingutil/service/resolver/builder.go:32-38` `NewSessionBuilder` | 初次 Get 可选 prefix、`WithSerializable`；Watch 从 `revision+1` 起，可选 prefix；结果传给 resolver state callback | 初次 KVs 的 Key/Value、Header revision；watch response Header revision；PUT `Kv.Key/Value`，DELETE key；resolver 消费版本化 session/address 状态 | Get/watch/context 错误返回；损坏 session JSON 记录并跳过该 session；watch 错误返回给上层，不在该符号内自动重试。
| U10 / `M0-CONFIG-POLL` | `pkg/config/etcd_source.go:146-186` `refreshConfigurations*` | 每次刷新 `Get(path.Join(keyPrefix,"config"), WithPrefix)`；后台周期刷新使用 Serializable；timeout 3s | 每个 KV Key/Value；成功构建整张新 map 后原子替换并通知 cache/events | Get/事件构造错误返回，不替换旧 map；周期 refresher 管理后续调用。范围读取无分页。
| U11 / `M0-DATACOORD-RECOVERY` | `internal/datacoord/meta.go:250-310` `reloadFromKV`；`internal/metastore/kv/datacoord/kv_catalog.go:115-220` `listSegments`、`listBinlogs` | 每个 collection 并发 `ListSegments`，并发池上限来自 `MetaStoreCfg.ReadConcurrency`；catalog 用 `WalkWithPrefix` 和配置的分页大小扫描 segment/binlog 范围 | callback 消费 KV key/value 并反序列化 segment/binlog 元数据 | Future 错误经 `AwaitAll` 返回，恢复失败不吞掉；读取并行度可配置。扫描本身分页，collection 粒度 fan-out 受池限流。
| U12 / `M0-GC-PREFIX-EXISTS` | `internal/metastore/kv/datacoord/kv_catalog.go:350-385, 788-803` `GcConfirm` 与写路径 | GcConfirm 对 collection/partition 前缀调用 `LoadWithPrefix`，物化所有匹配的 keys/values；部分写路径用 `HasPrefix` 判断 binlog/delta/stats 前缀 | GcConfirm 最终只检查 key/value 切片是否为空；`HasPrefix` 只消费存在布尔值 | GcConfirm 的现有实现会读取完整范围，调用方仅用结果判断是否为空。RPC 错误被映射为 `false`（未完成），不向上传递；改成存在性查询需作为独立兼容决策。`HasPrefix` 路径的错误则返回。
| U13 / `M0-LEGACY-TOMBSTONE-GC` | `internal/metastore/kv/rootcoord/legacy_tombstone_gc.go:45-170` | 对选定前缀 `WalkWithPrefix`，批次 `legacyGCBatchSize`，只收集 value 精确等于 tombstone marker 的 key，再按 `MaxEtcdTxnNum` 分批删除 | key/value；marker byte equality | 单轮错误记录并继续到下个周期；context cancel 停止；删除批次错误返回。该路径仍是当前启动的兼容清理逻辑，不能作为历史迁移入口排除。
| U14 / `M0-STREAMING-CATALOG` | `internal/metastore/kv/streamingcoord/kv_catalog.go:100-250`、`internal/metastore/kv/streamingnode/kv_catalog.go:50-260` | catalog list 使用 `LoadWithPrefix`；多值保存/删除用 batch helper 与 `MaxEtcdTxnNum` | keys、values；proto/JSON decode 后的 channel、pchannel、broadcast task、replicate 配置；保存失败返回 | Get/decode/encode/save 错误向上返回；不在 catalog 内隐藏。prefix list 是全量返回。
| U15 / `M0-WAL-CHECKPOINT-ORDER` | `internal/streamingnode/server/wal/recovery/recovery_background_task.go:125-156`；`internal/metastore/kv/streamingnode/kv_catalog.go` | checkpoint 持久化经过 StreamingNodeCatalog；先 salvage checkpoint，成功后再保存 consume checkpoint；retry operation 具有 backoff | checkpoint message ID、timetick、replicate checkpoint | 错误保持重试并阻止后续 checkpoint 推进；需保留持久化顺序。存储具体 RPC 由 Catalog/MetaKv 包装层负责。
| U16 / `M0-CDC-REVISION-CAS` | `internal/cdc/meta/replicate_meta.go:62-110` | prefix list 用 `WithPrefix + SortByKey/Ascend`；删除 transaction compare `ModRevision(key)==observedRevision` 后 `OpDelete` | `Kvs[].Key/Value/ModRevision` 和 Header revision；Txn `Succeeded` | Get/Txn 错误返回；比较不成功作为 `false`，保护异步删除不覆盖更新值。
| U17 / `M0-TSO-LOAD` | `internal/tso/tso.go:77-92` `timestampOracle.loadTimestamp`、`saveTimestamp` | exact-key `TxnKV.Load(context.TODO(), t.key)`；成功加载为空值时返回 zero time；保存走 exact-key `TxnKV.Save` | 二进制 big-endian Unix-nanosecond timestamp | **错误被吞掉：** Load 错误被当作 zero time 且返回 nil，初始化继续按 wall clock 生成并保存新值。M0 兼容需明确此失败语义，J66 风险登记；此项不是 `tso_util` timestamp 编解码。
| U18 / `M0-CONFIG-TXN` | `pkg/config/manager.go:509-632` `SaveImmutableConfigs`、`SaveConfigToEtcd`、`AlterConfigsInEtcd` | 新 key 以 `CreateRevision(key)==0` 比较后 `OpPut`；批量 update/delete 用一个 Txn 提交多项 `OpPut/OpDelete` | `TxnResponse.Succeeded`；提交后线性一致刷新读取对应 values | 单项写错误收集后最终返回汇总；已有 key 视作幂等跳过。immutable 配置刷新错误只记录 warning；其它 Txn 和刷新错误返回。
| U19 / `M0-CDC-WATCH` | `internal/cdc/controller/controller.go:70-180` `startWatchLoop`、`watchEvents`、`watchLoop`；`internal/cdc/meta/replicate_meta.go:25-55` `ListReplicatePChannels` | 先 prefix Get 并按 key 升序；从 list Header revision + 1 起 Watch `WithPrefix, WithPrevKV, WithRev` | list 的 Key/Value/ModRevision/Header revision；PUT 用 `Kv`；DELETE 用 `Kv.Key` 与 `PrevKv.ModRevision` | list 错误记录后循环重试；watch error 返回外层重列举；关闭 channel panic；单条 replicate proto 解码失败由 MustParse panic。取消时退出。
| U20 / `M0-MANAGEMENT-SESSION-CLEAN` | `cmd/milvus/util.go:197-228` `CleanSession`、`getSessionPaths` | 使用 remote etcd client；找到 session suffix 后逐个 exact-key Delete；5s context | delete response 未消费 | **错误被丢弃：**每个 Delete 的 error 被 `_` 忽略，函数仍返回 nil。属于人工运维入口，不计作服务运行正确性证据；需保留失败可见性风险。
| U21 / `M0-MANAGEMENT-MCK` | `cmd/milvus/mck.go` `mck.run` | 通过 MetaKv 对 segment、collection snapshot、trigger task、active task 等固定前缀执行 `LoadWithPrefix` | values 反序列化为 protobuf；按 task/segment/partition 关系检查无效任务 | 列表错误触发 fatal；单项 decode 错误记录并跳过。属于人工维护诊断命令，prefix 全量读取规模需运维评估。
| U22 / `M0-AUTH-BRIDGE-CLIENT` | `pkg/util/etcd/etcd_util.go:102-107, 165-186` `GetRemoteEtcdClientWithAuth`、`CreateEtcdClient`；上游 `client/v3@v3.5.23/client.go:275-287, 398-400` 与 `auth.go:135-137` | Milvus 将 username/password 写入 `clientv3.Config`；固定 Go client 在建立 token bundle 时调用 `Authenticate(name,password)`，再为后续 RPC 附加 token credential | Authenticate token 与每个 RPC 的 authorization metadata | 凭据由固定 client 自动使用；认证失败由 client 返回错误。B03 仍需证明目标 HBase 机制能接受并授权这些凭据、token 跨副本可用；不从配置用户名密码推断 HBase 认证已经完成。

## 全仓搜索范围与排除

执行了固定版本下 `rg` 搜索：`clientv3`、`go.etcd.io/etcd/client/v3`、`WithPrefix`、`WithRange`、`WithRev`、`WithCountOnly`、`WithLimit`、`WithPrevKV`、`KeepAlive`、`.Watch(`、`.Compact(` 以及直接 client 方法，并逐项复核 DEVELOPMENT_PLAN.md §13.1 清单和上述生产调用路径。

- `cmd/tools/migration/backend/etcd.go`、`etcd210.go`、`cmd/tools/migration/migration/runner.go` 是显式 migration 工具；可执行迁移流程并非常规服务运行调用，已从首版 runtime consumer 表排除，但 migration 后端的 Count/Range 语义仍须在它作为工具受支持时单独评审。
- `*_test.go`、`internal/*/mocks/`、`pkg/mocks/` 和 test utility 仅作为测试基础设施，不作为生产消费者计数。
- `internal/kv/etcd/embed_etcd_kv.go` 是 embedded etcd 后端实现，需在本服务协议/生命周期实现中单独兼容；本表优先列出共用 `etcd_kv.go` 抽象与消费端。
- 只搜到业务名为 Compact 的向量/数据 compaction，没有生产路径调用 etcd `Maintenance/Compact`。因此首版 Compact RPC 尚无 Milvus consumer evidence；WP12 必须将它列为显式拒绝/不支持，直到补充调用依据。
- `Watch` 命中中还包括 config manager、streaming client、MQ/flush 等非 etcd watcher，以及 mocks；只有传入 etcd client 或上述 etcd-backed wrapper 的调用纳入本清单。

## 已知协议观察与待互通验证

- `Get` 的 prefix、range/排序/分页选项改变结果范围和顺序；不能仅按方法名等价。
- `WithCountOnly` 既用于 key/prefix 存在性，也在 migration 工具中取 Count；要结合调用消费者区别。
- Watch 初始列表与 revision+1 的连接、`PrevKV` 删除事件、compact 后重列举，是 session/proxy 发现的核心状态恢复契约。
- Session Lease 注册与续约不是简单单 key Put；包括多角色协调器事务、Lease ID、revision 和按错误类型区分的 retry。
- CDC 删除的 ModRevision compare 与 Streaming WAL checkpoint 持久化顺序会影响状态安全，后续 driver 必须捕获比较失败、流终止和恢复场景。
- 已登记 J66 错误消费风险：TSO `loadTimestamp` 丢弃 KV 读取错误后继续初始化；`GcConfirm` 把范围读取错误映射为 `false`；Session/Streaming watcher 对单条损坏 session 记录日志并跳过；config immutable 检查/写后刷新错误仅记录后继续；管理命令删除失败被丢弃。后续等价行为不得简单假设为 gRPC status 透传。
- 此文是静态源审计；尚无 Go/Java 互通或真实 HBase 结果。实现中发现的 `context.Background()` watcher、错误消费和 bool-only `GcConfirm` 由后续 J14/J66 等任务验证/处理，不在 Milvus 中改动。

## 计划中的公开边界

- **Count：** 遵循 DEVELOPMENT_PLAN.md §4.1：精确 key `CountOnly` 返回真实 0/1；prefix `CountOnly + Limit=1` 只作为存在性查询；普通未分页范围 Count 为返回 KV 数，分页 Count 为本页 KV 数且 `More` 必须真实；其它范围 CountOnly 首版明确不支持。U03/U12 与 migration 工具用量要在 WP12 的消费者矩阵中分别映射，不能把存在性查询当准确总数。
- **Auth：** 固定 Go client v3.5.23 在配置 username/password 后会请求 `Authenticate` 并给后续 RPC 附带 token。服务需要把凭据安全桥接为 HBase 后端身份；它不代表提供原生 etcd RBAC。用户/角色管理 RPC 按 DEVELOPMENT_PLAN.md §5.4 / J51 逐项映射或拒绝。B03 和目标 HBase 身份证据仍缺。
- **Kafka：** Kafka 是 Milvus 集成验证环境的依赖；兼容服务本身不依赖 Kafka，也不解析 Kafka/Woodpecker 业务消息，依据 DEVELOPMENT_PLAN.md D04。Kafka 环境 smoke 属于后续接入验证，不进入服务运行时协议消费者清单。

## 首版处理状态

U01–U22 固定的是当前 Milvus 的语义候选；首版 API/参数组合的最终 `supported` / `UNIMPLEMENTED` 判定由 WP12 的 `docs/api-matrix.md` 完成。无生产 etcd Compact consumer evidence；Compact 必须保持矩阵中的明确不支持结果，除非新增已审计调用和评审决定。此静态 usage 清单本身不声明完整兼容验收通过。
