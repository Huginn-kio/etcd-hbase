# M4 Lease 与事件历史开发配置

启用 `m4_lease_api_enabled` 时，`memory_bytes` 必须同时覆盖每个表组的 Lease owner、expiry worker、事件回填和 EventTailer 预留，以及普通工作账户所需的生命周期内存。运行时在扣除 owner transport 预留后，按以下规则校验最低值：

```text
所有表组的 Lease/到期/事件回填保留容量
+ max(
     所有表组的恢复账户 + 一组 Lease gateway/owner 请求账户,
     所有表组的恢复账户 + 顺序启动 Lease 扫描账户
  )
```

保留容量和扫描/请求账户大小由表组数量及 `max_etcd_request_bytes` 等预算参数计算。各表组顺序启动，启动扫描完成并通过 readiness 后才开放请求，因此扫描与 Lease 请求按两种互斥阶段取较大值；恢复账户在两阶段都计入。配置不足时，启动会给出计算出的最低字节数并拒绝继续。示例 `memory_bytes` 为 1,073,741,824 字节，并为 owner forwarding 保留空间；实际下限会按运行时并发数和请求大小重新计算。

活跃 Watch 的订阅范围与索引登记从各自请求账户的 OUTPUT 预算中计费：每个订阅按 512 字节固定索引/对象开销，加上实际保留的 lower/upper key bytes 预留。预算不足时拒绝创建订阅；取消或流关闭时释放该预留。该账本下限仍不代表所有并发请求、其他工作负载、JVM 或 HBase client 的完整内存上限，实际部署需按并发和目标环境容量单独评估。
