# M0 公开 SPI 契约

J08 核对当前真实调用链，保留必要的公开面；本层不引入通用插件框架。`spi` 生产代码只使用公开 JDK 类型，HBase 专属 API 留在 `hbase.api`。接口能编译不表示目标认证或发布兼容已验收。

## 1 公开面与实际使用

**表 1 公开 SPI 与实际调用边界**

| 接口或 DTO | 现有调用与边界 |
| --- | --- |
| `SaltStrategy` / `SaltProfile` | J05 初始化计划经 `InitializationConfigurationValidator` 使用公开 CRC32 候选；后续编码/扫描按相同 profile 使用。第三方策略可实现接口，但当前配置尚不装载自定义策略。 |
| `BackendIdentityProvider` | J04 `HBaseBackendIdentityProvider` 实现认证、真实后端权限检查和有界资源管理；J05 初始化 CLI 经该 provider 借用授权连接。它不签发 etcd token。 |
| `BackendCredentials` | 调用方持有原对象；provider 在异步返回前复制所需 secret，并在使用结束后清零复制值；调用方在 stage 完成后关闭原对象。 |
| `AuthenticationScope` | 一个配置表组和连接 profile；不能以请求里声称的账号、表组代替后端授权。 |
| `AuthorizedBackendIdentity` | provider 签发的授权 handle，携带 scope、不可变允许组集合与凭据代次。DTO 可构造，但 HBase 工厂还须核对其实际签发状态；DTO 本身不是认证证明。 |
| `BackendIdentityException` | 六类稳定失败：凭据拒绝、权限不足、资源上限、超时、取消、后端不可用；消息不携带凭据或原始后端异常。 |

HBase 的 `HBaseConnectionFactory`、`HBaseCredentialAuthenticator`、`HBaseUserContext`、`HBaseAccessVerifier` 使用 HBase/Hadoop 类型，因此位于 `hbase.api`。当前一个 `HBaseBackendIdentityProvider` 同时实现身份 provider 和连接工厂，以同一所有者验证签发状态和管理资源。

## 2 标识、版本与能力

- provider ID 和 strategy ID 是配置中的稳定标识，采用现有有界 ID 规则。配置装配显式选择，未知或未装配能力失败；不得依赖发现顺序或匿名回退。
- `SaltProfile.encodingVersion` 标识持久化字节格式。当前公开候选为 `crc32-bucket-v1`、版本 1、16 桶、2 字节大端；目标 B01 尚未确认。算法、完整逻辑输入、桶数或编码改变都需要新 profile 和迁移，不在线静默替换。
- `credentialGeneration` 标识同一授权上下文的凭据代次，不是 SPI 版本或 etcd revision。当前 HBase provider 签发代次 0；轮换、逐会话退役及跨副本身份恢复仍归 J50/J51。
- Java 接口兼容版本随将来的公开 artifact 版本管理；当前只有本地 `0.1.0-SNAPSHOT` 工程坐标，I08 尚未确认。M0 不虚构一个已发布的二进制兼容承诺，也不把格式版本当成接口协商机制。
- 标准 SIMPLE 开发、Kerberos、delegation token、账号密码 DIGEST 与自定义认证适配的当前能力见[身份边界](../hbase/j04-backend-access.md)。DIGEST 适配不是产品只支持 DIGEST；真实目标机制、权限和 TLS 仍待输入与验证。

完整 provider 发现/装载、能力协商、发行兼容与升级演练由 J18/J19/J58 交付。当前运行服务的安全身份激活/TLS 尚未接入；离线配置校验通过不能当作已启动安全服务。

## 3 线程、生命周期与所有权

盐策略可并发复用，计算只做有界 CPU 工作，不发起后端 I/O；返回的盐和 split key 数组由调用方拥有。输入不得修改，完整逻辑标识、点读和范围边界使用相同 profile。当前 Hash 实现每次使用独立 CRC32 状态，并限制 split 枚举容量。

身份 provider 也可并发复用。`authenticate` 应迅速返回非空 stage；真实阻塞 I/O 使用 provider 自持的有界执行器。成功前必须完成凭据转换、连接创建与实际后端权限核实；本地创建一个 User 或复制 DTO 不构成成功证据。

**表 2 资源所有者与释放责任**

| 资源 | 所有者及释放 |
| --- | --- |
| 原始 `BackendCredentials` | 调用方；stage 结束后关闭，关闭后拒绝复制 secret |
| provider 复制的 secret / 用户上下文 | provider；secret 用毕清零，用户上下文在借用连接关闭后释放 |
| 共享 `AsyncConnection` | HBase 工厂；请求只借用，不关闭连接或取消共享源 future |
| 请求 scanner / 请求操作 future | J06 `RequestScope` / `HBaseRequestContext`；取消与 cleanup 只作用于请求所有资源 |
| provider 的执行器和保留 context | provider；容量有界，关闭停止新工作并完成或取消未完成工作，重复关闭安全 |

当前 SPI 没有逐身份释放回调；J04 provider 以最大 context 数量约束保留状态，关闭时清理。这里不承诺长时间身份轮换或跨 Pod 复用已经实现；会话层不得超期使用身份。取消 stage 也不能撤销已由 HBase 接受的 mutation。

## 4 验证

`testkit` 中的外部 provider/盐实现只使用 JDK 与 `spi` 类型，验证调用方凭据所有权、不可变授权集合、失败分类、重复关闭和返回数组所有权。它们是契约夹具，没有连接 HBase，也没有安装为生产 provider。

```bash
scripts/verify/j08.sh
python3 scripts/verify/m0-spi.py
ETCD_HBASE_JDK21_HOME="$JAVA_HOME" python3 scripts/verify/m0-spi.py --jar spi/target/spi-0.1.0-SNAPSHOT.jar
```

边界入口检查八模块 POM 与生产源码，拒绝 SPI 的非 test 依赖、反向项目依赖和传输/存储类型泄露；打包后另用锁定 JDK 的 `jdeps` 检查实际 SPI JAR 仅依赖公开 JDK 模块。常用 Maven group/parent group 表达式按当前工程组解析，其他未解析坐标明确失败，不把它们当成外部库绕过检查。静态检查、第三方夹具、J04 的真实临时 HBase 认证及目标环境验收分别报告。
