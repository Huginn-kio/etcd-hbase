# J04 后端身份与 HBase 基础探针

J04 建立后端身份契约、有界连接复用和真实临时 HBase 探针。gRPC 业务方法继续按 J03 返回 `UNIMPLEMENTED`；本任务不发放 etcd token，也不把凭据封装或建连成功视为认证、授权成功。

## 身份与连接边界

- `spi.identity` 仅依赖 JDK，提供可关闭的 `BackendCredentials`、不可变的 `AuthenticationScope` / `AuthorizedBackendIdentity` 及 `BackendIdentityProvider`。
- provider 验证后端身份及表组权限后才能返回授权 handle。HBase `User`、UGI、token 和连接留在 HBase 层，不放进通用 SPI 或日志。
- `OfficialHBaseConnectionFactory` 按 provider、subject、context、表组、连接 profile 和 credential generation 复用连接；达到容量上限时明确拒绝，不驱逐仍被借用的连接。借用者不能关闭共享连接。
- 单个借用者取消不取消共享建连。建连超时返回 `TIMEOUT`，底层工作仍占容量，直至失败或迟到连接被关闭，避免反复超时产生无界后台连接。
- provider 的连接 creator 必须及时返回异步 stage；阻塞工作使用 provider 的有界执行器。future 回调不得在 HBase 或 deadline 线程上执行阻塞操作。

## 认证接入范围

认证方式由 backend client/profile 和 provider 决定，不在通用 SPI 中把集合封闭为 DIGEST。连接 creator 可使用官方 `ConnectionFactory.createAsyncConnection(configuration, user)` 接入 provider 持有的 HBase `User`。

| 路径 | 当前交付与验证 |
|---|---|
| 显式开发无认证 | `DevelopmentIdentityProvider` 仅接收空密码并返回明确的 synthetic identity。默认开发连接只允许 loopback ZooKeeper 和 simple 配置；不是目标认证的 fallback。 |
| 用户名/密码 DIGEST | `DigestPasswordUser` 为明确使用账号密码 DIGEST 的后端构造独立、可关闭的 token/User 凭据载体。公开 HBase SASL client 的成功和错误密码交互已由协议 fixture 验证；尚未证明目标 HBase 的身份及权限。 |
| Kerberos / HBase delegation token / 自定义 SASL | 通过 provider、官方 client 及各自的 User/凭据上下文接入；本轮未执行对应真实环境验证，不宣称已兼容所有实现。 |

标准 HBase DIGEST 通常用于 delegation token。`DigestPasswordUser` 不获取 delegation token，也不能让标准 token 服务端接受任意账号密码。账号密码后端必须具有相应校验语义，使用实际 HBase cluster ID 和适配的安全配置；provider 必须通过真实授权操作确认结果，不能仅创建 User 后返回成功。

凭据载体复制并持有 UTF-8 密码字节，不把密码放入 `Configuration` 或 `String`。拥有者先关闭使用该身份的连接，再关闭载体；原始 `BackendCredentials` 仍由调用者关闭。`HBaseFailures` 区分认证、权限、超时、取消、不可用和未知失败，公开消息不回显后端诊断。

## 临时环境验证

按项目工具链准备锁定的 Temurin JDK 21，设置 `ETCD_HBASE_JDK21_HOME`，运行：

```bash
scripts/verify/hbase-probe.sh --profile public-local
```

入口执行 Java 21 构建、身份与连接单测、DIGEST SASL fixture，以及独立 JVM 中的真实临时 HBase。服务器和客户端 classpath 分离，临时服务绑定 loopback，使用 simple 无认证配置。测试创建专用随机 namespace/table，清理自身资源，不访问外部集群。

真实 HBase 场景覆盖二进制 Put/Get、CAS 匹配与不匹配、有界 Scan、同表不同运行隔离、独立连接与重建连接读回、精确行清理和连接关闭。Scan 限定三行、缓存三行及结果大小；操作与扫描设置 client 超时。探针记录表级及 mutation 级 `SYNC_WAL`，不以重连读回证明 RegionServer 故障耐久性。

超时、取消隔离、迟到连接清理和关闭竞争另有确定性单测。runner 拒绝零测试、失败或跳过，保留本次 JUnit XML、摘要及 SHA-256 到本地 `.artifacts/m0/j04/`；原始产物不进入仓库。

client/Hadoop 版本来自依赖锁，服务端版本从临时环境采集。版本锁用于可重复构建，不要求目标 HBase 与 client 同版本，也不要求目标服务端使用 JDK 21。候选组合通过不代表所有版本均受支持，兼容边界参见 [HBase Java 支持矩阵](https://hbase.apache.org/docs/configuration/basic-prerequisites/)。

## 目标环境待验证项

本轮先验证临时环境。目标入口、DIGEST 凭据校验与 cluster ID、专用表/列族及权限、TLS/凭据引用和获批负例仍需确认；`--profile target` 明确返回非零状态，不用 public-local 替代。

实际地址、账号、密码、token、证书和 keytab 内容不进入代码、示例或 PR。后续使用受控外部配置及安全引用接入。跨副本 session、身份轮换、后台激活和完整 token 生命周期仍归 J50/J51。

相关契约参见 [M0 设计](../specs/2026-10-10-m0-engineering-environment-protocol-design.md)、[开发计划](../../DEVELOPMENT_PLAN.md) 和 [HBase SASL provider API](https://hbase.apache.org/2.6/devapidocs/org/apache/hadoop/hbase/security/provider/package-summary.html)。
