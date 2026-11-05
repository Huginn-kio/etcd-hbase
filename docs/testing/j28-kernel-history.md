# J28.1 内核最小历史模型

J28.1 在 M2 验证最小 Compare/Put/Delete/Read 操作历史。受限测试驱动通过生产 `CommitPublisher` 发布，通过 `ReadViewService` 和 `ReadViewRecordStore` 读取，并通过 `RecoveryManager` 和 `CommitFinalizationService` 恢复。记录的历史交给独立串行参考模型搜索；调用、返回和已知发布决定时点从实际操作及控制行更新处记录。

模型覆盖一个并发 Compare/多 key Put：第一个候选部分落地后，真实请求作用域取消并使调用方 future 以取消结束；模型将调用方未获结果记为 unknown，并从控制行单独观察到已选发布。与其重叠以及其后发生的读取都必须看到已选发布里的两把 key；随后停止测试中的服务作用域，恢复重放不重复分配 sequence/revision。后续 stale Compare 失败，独立 Put 和 Delete 分别由后续 Read 观察。单独的 unknown 用例保留两种可能：结果未知本身不能推出已执行或未执行；后续读取决定哪些串行解释可接受。

## 1. 验证

使用项目锁定的 Temurin JDK 21，在仓库根目录运行：

```sh
scripts/verify/j28.sh
```

脚本清理并运行 `core` 全部单测，重点确认 J28 历史模型以及 J25 `ReadViewRecordStore`、J27 `RecoveryManager` 回归；本地原始记录写入被忽略的 `.artifacts/m2/j28/`。

## 2. 证据边界

这是有界内核模型，不是穷举无界状态。候选和清单准备、存储、读视图 pin 与请求到服务的交接使用确定性的测试夹具；Compare 由驱动在真实读视图结果上执行，尚非完整业务事务或 RPC。它不验证真实 HBase RPC 的网络结果未知、跨进程崩溃、HDFS/WAL 持久性、Go client 或 Milvus 行为。真实 Go client 并发历史属于 J28.2/M3。J27 的 MiniCluster 和恢复服务测试单独覆盖实际 HBase 适配器的恢复路径。
