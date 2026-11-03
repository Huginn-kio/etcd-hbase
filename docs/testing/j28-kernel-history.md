# J28.1 内核最小历史模型

J28.1 在 M2 验证最小 Compare/Put/Delete/Read 操作历史。它使用受限测试驱动调用生产 `CommitState` 的 owner、publish 和 finalize 转移，再把记录的操作交给独立串行参考模型搜索。每项历史保留调用时间、返回时间、已知发布决定时点、返回状态和读到的值。

模型覆盖一个并发 Compare/多 key Put：第一个候选部分落地后，请求被取消并返回 unknown；与其重叠以及其后发生的读取都必须看到已选发布里的两把 key；恢复重放不重复分配 sequence/revision。后续 stale Compare 失败，独立 Put 和 Delete 分别由后续 Read 观察。单独的 unknown 用例保留两种可能：结果未知本身不能推出已执行或未执行；后续读取决定哪些串行解释可接受。

## 验证

使用项目锁定的 Temurin JDK 21，在仓库根目录运行：

```sh
scripts/verify/j28.sh
```

脚本清理并运行 `core` 全部单测，重点确认 J28 历史模型以及 J25 `ReadViewRecordStore`、J27 `RecoveryManager` 回归；本地原始记录写入被忽略的 `.artifacts/m2/j28/`。

## 证据边界

这是有界内核模型，不是穷举无界状态，也没有驱动生产 `CommitPublisher` 的完整准备/发布流程或真实 HBase RPC。它不验证网络调用结果未知、跨进程崩溃、HDFS/WAL 持久性、Go client 或 Milvus 行为。真实 Go client 并发历史属于 J28.2/M3。J27 的 MiniCluster 和恢复服务测试单独覆盖实际 HBase 适配器的恢复路径。
