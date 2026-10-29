# J04 后端认证、身份与 HBase 基础探针

J04 提供统一后端认证入口、有界身份连接管理和真实临时 HBase 探针。认证方式由显式选择的适配器决定，测试环境使用 DIGEST 不限制产品的认证范围。gRPC 业务方法继续按 J03 返回 `UNIMPLEMENTED`；etcd token/session、跨副本和身份轮换归 J50/J51。

## 1. 接入与授权边界

`spi.identity` 仅依赖 JDK，提供可关闭的 `BackendCredentials`、不可变的 `AuthenticationScope` / `AuthorizedBackendIdentity` 及 `BackendIdentityProvider`。HBase `User`、UGI、Kerberos Subject、后端 token 和连接只存在于 HBase 层。

`HBaseBackendIdentityProvider` 同时实现通用身份 provider 和 HBase 专属连接工厂，负责以下顺序：

1. 在有界工作线程中将提交的凭据转换为独立 `HBaseUserContext`。
2. 通过官方 `ConnectionFactory.createAsyncConnection(configuration, user)` 建立对应身份的连接。
3. 使用该连接实际验证表组所需权限，成功后才返回授权身份；创建 User 或连接不等于认证和授权成功。
4. 同一授权身份复用已验证连接，其他身份、表组和连接 profile 不复用该连接。借用者不能关闭共享连接。

一个实例绑定一个 `AuthenticationScope`。作用域不匹配、未由该实例签发的身份、容量已满或实例已关闭均明确拒绝。身份上下文、凭据副本、工作队列和线程数有上限；单次认证覆盖凭据转换、建连和权限 RPC 的总超时最长五分钟。

取消或超时会立即结束调用者的等待并中断仍在执行的凭据加载。尚未结束的底层工作继续占容量，迟到的 User/连接会被关闭；不能取消原始建连 future 后丢失迟到资源。关闭连接失败时保留上下文及容量，关闭 provider 时重试；先关闭连接，再销毁凭据。关闭期间未结束的底层调用仍由 provider 跟踪至完成，不能把调用者已取消写成资源已经释放。空闲工作线程会回收。

保留的已授权上下文在 provider 关闭时释放；M0 的容量上限不替代 J50/J51 的会话过期、逐身份退役、凭据更新或 Kerberos 续期。

## 2. 认证方式

**表 1 后端认证方式与公开验证范围**

| 方式 | 入口与凭据 | 当前公开验证 |
|---|---|---|
| SIMPLE | 显式 `HBaseAuthenticators.simpleDevelopment()`，仅空密码、loopback ZooKeeper 和无认证/无授权开发配置；SIMPLE 不能证明所声称用户名的归属 | 统一 provider 连接真实临时 HBase，独立用户连接和 Get/Put/CAS/Scan；不是目标 ACL 证明 |
| Kerberos 密码 | `kerberosPassword()`，提交完整 `principal@REALM` 和密码；独立 JAAS Subject，无全局登录用户替换 | 临时 KDC 与启用 ACL 的真实 HBase：服务端确认 `KERBEROS`，执行 Get/Put/CAS/Scan、独立连接读回；错误密码、只读与无权限账号被拒绝 |
| Kerberos keytab | 受控 `KerberosUserContext.fromKeytab(principal, path)`，配合 `activateTrusted` | 临时 KDC 与真实 HBase：服务端确认 `KERBEROS`，完成上述操作及权限检查 |
| Kerberos 票据缓存 | 受控 `KerberosUserContext.fromTicketCache(principal, path)`，配合 `activateTrusted` | 临时 KDC 生成缓存，真实 HBase 确认 `KERBEROS` 并完成上述操作；单测另覆盖缓存身份不匹配拒绝 |
| HBase delegation token | `delegationToken(actualClusterId)`，secret 为标准 Hadoop URL 编码的既有 token | 通过真实 HBase 的 `TokenProvider` 签发，服务端确认 `TOKEN`，完成上述操作；篡改 token 被拒绝，长度/owner/kind/service/expiry 另有单测 |
| 账号密码 DIGEST | 显式 `digestPassword(actualClusterId)`，仅面向实现相应密码校验语义的后端 | 真实 HBase 装载测试用独立 SASL 扩展，完成上述操作；错误密码、无权限账号被拒绝；标准 token provider 正确拒绝账号密码载体 |
| 自定义 SASL/provider | 实现 `HBaseCredentialAuthenticator` 和有所有权的 `HBaseUserContext`，配合官方 HBase SASL provider/selector；自定义权限系统提供实际 RPC verifier | 上述账号密码测试扩展在真实 HBase 中与内置 Kerberos/token provider 共存；其他扩展仍需逐个验证 |

标准 HBase 的 DIGEST 用于 delegation token，不能接受任意账号密码。`digestPassword` 和 `delegationToken` 是不同的凭据适配器，不能互相替代。两者都使用实际后端 cluster ID，且不获取或伪造服务器签发的 delegation token。

安全路径采用公开 HBase client 的 SASL 配置，并禁止 `hbase.ipc.client.fallback-to-simple-auth-allowed`；认证失败不会换用其他适配器或降级为 SIMPLE。公开 client 的 `hbase.security.authentication=kerberos` 开启 SASL，也用于 delegation-token/DIGEST 用户，不应为其自行发明 `digest` 配置值。TLS、SASL QOP、服务 principal 和信任材料按实际后端受控配置设置。

Keytab/票据缓存是服务端受控身份来源，不能把“客户端提交一个用户名”当作借用这些凭据的授权。`activateTrusted(() -> KerberosUserContext.fromKeytab(...))` 只用于已获授权的初始化/后台激活；仍须经过相同的后端权限检查。网络 Authenticate 始终走客户端凭据验证路径。

自定义 SASL 通过官方 `SaslClientAuthenticationProviders.EXTRA_PROVIDERS_KEY` 与 `SELECTOR_KEY` 配置。当前锁定的 HBase 2.6.6 client 在 `AbstractRpcClient` 构造时根据连接配置创建 provider registry；扩展类与 selector 必须在创建对应连接前配置。selector 应同时识别内置和已安装扩展，不通过 reset 全局 registry 切换机制。凭据适配器可以增加新机制，不需要修改通用 SPI、gRPC 或封闭的认证枚举。

账号密码集成测试为扩展分配独立的 SASL method code `93` 和 token kind `ETCDHBASE_TEST_PASSWORD_DIGEST`，只将该 kind 路由给测试扩展；内置 delegation-token provider 的 code `82` 不变。服务端使用生成的测试账号和固定测试密码验证 DIGEST-MD5，不接入目标账号数据库。扩展类、账号与凭据只存在于测试源码和临时目录，不进入运行制品。

## 3. 权限与凭据

`HBaseAccessVerifiers.tablePermissions` 根据配置的每张表及所需 action，调用当前用户的真实 HBase `hasUserPermissions` RPC。每项都成功才发放授权身份；缺失、部分拒绝或不支持该 ACL RPC 时失败。自定义后端必须提供自己的实际权限 verifier，不能用“连接成功”或“读到空结果”替代表组读写权限证明。临时 SIMPLE 环境的 Get 只验证显式开发路径的可达性。

凭据在异步返回前复制，受长度约束并在使用后清零；后端密码不放入 `Configuration` 或 String。token 解码在分配数组及调用 HBase identifier decoder 前核对输入长度，验证 owner、kind、cluster 和有效期；服务器仍负责验证真实 token。凭据不进入 SPI 身份 handle、日志、异常公开消息或 etcd token。

`HBaseFailures` 区分认证、权限、超时、取消、资源容量、不可用和未知失败。连接和身份上下文统一由 `HBaseBackendIdentityProvider` 持有；生产适配与临时探针使用同一套容量、超时、签发身份和关闭机制。开发模式只通过显式 `simpleDevelopment` 适配器进入统一路径。

安全集群关闭 SIMPLE 回退，并用实际 SIMPLE 客户端 RPC 验证拒绝。在当前固定 client 中，服务端拒绝 RPC preamble 后，客户端只暴露 `ConnectionClosedException`，因此保留 `BACKEND_UNAVAILABLE` 分类；不能根据错误文本猜测认证原因。错误 Kerberos 密码、篡改 token 和错误 DIGEST 密码另有实际认证失败分类断言，权限不足也单独验证。

## 4. 验证入口与证据

按项目工具链准备锁定的 Temurin JDK 21，设置 `ETCD_HBASE_JDK21_HOME`，运行：

```bash
scripts/verify/hbase-probe.sh --profile public-local
```

入口执行 Java 21 构建、认证与连接单测，以及 SIMPLE、安全两组独立 JVM 的真实临时 HBase。安全组在同一临时环境中启动 KDC、HBase Master/RegionServer、`AccessController` 和 `TokenProvider`，生成服务/测试用户的 keytab 与票据缓存，为专用表显式授权。测试不访问外部集群，临时服务绑定 loopback，HBase 数据和 WAL 使用本地文件系统。

安全集成场景对每种机制调用服务端 `TokenProvider.WhoAmI`，断言实际用户名及 `KERBEROS` / `TOKEN`，再执行 Get/Put/CAS/Scan 和独立管理员连接读回。token/DIGEST 身份明确没有 Kerberos TGT，避免悄然改走 Kerberos 后误报通过。只读及无权限账号不能取得读写授权身份。

测试服务端使用非 shaded 的 HBase 测试组件，其 ACL 存储路径依赖 protobuf 2.5.0 ABI。Maven 只在测试资源阶段将已锁定摘要的旧运行库复制到 `target/server-libs/`，子进程 classpath 排除应用 protobuf 后载入该文件；应用继续使用原 protobuf 4.36.2。测试日志实现同样仅为 test scope。不能用降级应用 protobuf 或关闭 ACL 掩盖服务端兼容问题。

真实 HBase 场景覆盖统一 provider、不同身份连接、二进制 Put/Get、CAS 匹配/不匹配、有界 Scan、同表不同运行隔离、独立/重建连接读回、精确清理与关闭。操作有 client 超时，Scan 限定三行、缓存三行及结果大小；表及 mutation 使用 `SYNC_WAL`，不以重连读回证明 RegionServer 故障耐久性。

超时、取消、阻塞登录、迟到连接、权限 RPC 中关闭、清理失败重试和伪造 handle 另有确定性单测。完整公开入口当前执行 48 项测试，其中 SIMPLE 集成 1 项、安全集成 6 项；runner 拒绝零测试、失败、跳过、缺少认证套件或实际机制/负例证据，保留 JUnit XML、服务端日志、摘要及 SHA-256 到本地 `.artifacts/m0/j04/`。临时 KDC/keytab/cache 随测试关闭清理，原始产物不进入仓库。

client/Hadoop 版本来自依赖锁，服务端版本由临时环境采集。锁定用于可重复构建，不要求目标 HBase 与 client 同版本，也不要求目标服务端使用 JDK 21。候选通过不等于所有版本受支持，兼容边界参见 [HBase Java 支持矩阵](https://hbase.apache.org/docs/configuration/basic-prerequisites/)。

## 5. 目标环境待验证项

公开验证不替代目标 HBase 验收。目标入口、实际认证/profile、cluster ID、专用表/列族权限、TLS/安全凭据引用及获批负例仍待提供；`--profile target` 明确返回非零状态。公开临时环境未覆盖安全 HDFS、ZooKeeper SASL、TLS、代理用户、凭据续期或所有自定义发行版；HBase RPC 认证成功不表示这些组合也已验证。真实地址、账号、密码、token、证书、keytab 和票据内容不进入代码、示例或 PR。

相关契约参见 [M0 设计](../specs/2026-10-10-m0-engineering-environment-protocol-design.md)、[开发计划](../../DEVELOPMENT_PLAN.md)、[HBase SASL provider API](https://hbase.apache.org/2.6/devapidocs/org/apache/hadoop/hbase/security/provider/package-summary.html) 和 [JDK 21 Kerberos LoginModule](https://docs.oracle.com/en/java/javase/21/docs/api/jdk.security.auth/com/sun/security/auth/module/Krb5LoginModule.html)。
