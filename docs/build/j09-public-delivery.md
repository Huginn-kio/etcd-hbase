# J09 公开构建与运行包

M0 当前提供配置校验、请求生命周期、协议骨架、HBase 基础接入和测试工具。生产 RPC 的业务 handler 返回 `UNIMPLEMENTED`。运行包的启动、错误互通与关闭证据不代表完整 KV、Txn、Watch、Lease 或 Auth 已实现，也不代表目标集群验收。

## 1 公开完整验证

需要 Python 3.11+ 和 POSIX 环境；公开 CI 使用 Ubuntu 24.04。使用 [锁定工具链](m0-toolchain.md) 配置 `ETCD_HBASE_JDK21_HOME` 后，在仓库根目录执行：

```bash
scripts/verify/m0.sh --scope public
```

入口要求源码已提交且无修改；本地执行恢复记录不参与构建。每次运行生成独立的 `.artifacts/m0/public/<run>/INDEX.json`。`scripts/verify/report-index.py --phase M0 --artifact-root .artifacts/m0/public/<run>` 会重算文件摘要，并再次核对真实 XML、Go 测试事件、运行包及镜像证据。本地进度记录已有 `verification_index` 时，也可使用 `--progress docs/progress/m0-execution.md`。

入口依次执行：

1. 验证工具的负例、公开依赖配置、Go 实际隔离环境、模块锁、API 清单和 proto 原始摘要。
2. 执行全部 Go 单测、race、vet；运行两次原生 embed etcd 场景并拒绝十种损坏 trace。
3. 使用 `m0-public` profile 执行两次 `clean install`。这包含完整 `verify` 生命周期和 J03/J07 三个互通 IT，没有跳过测试参数。核对每次实际测试报告，要求零失败、错误或跳过，并比较八个模块及 HBase test JAR 共九个 JAR 的 SHA-256。
4. 对编译后的 SPI 执行 JDK-only 边界检查；从已验证 JAR 构造运行包，启动真实 Java 进程，以固定 Go client 验证 bytes、int64、`UNIMPLEMENTED` 和有界退出。
5. 构建并运行固定镜像，核对实际 Java vendor/patch、非 root 用户、错误互通、SIGTERM 退出与资源删除；记录 I07/I08 决策状态和最终索引。

Maven SNAPSHOT 依赖安装到本工作树专用的 `.artifacts/m0/j09/maven-repository`，避免其他工作树覆盖同坐标产物。运行入口对本工作树加排他锁。Go 仅使用公开 proxy/checksum 服务，禁用私有包配置、VCS 回退和自动升级 toolchain。公开构建不需要目标凭据或扩展工程；J00 已接受的静态调用审计另有固定来源报告，此入口不重新克隆完整 Milvus 工程。

完整公开验证退出 `0`。本机 Docker CLI 或默认本地 daemon 不可用时，其他检查仍执行并记录镜像 `unavailable`，最终退出 `2`、状态 `partial`；失败检查退出非零。镜像配置文件存在或其他检查成功都不能代替实际镜像验证。J09 公开 CI 在 Ubuntu 上要求完整入口及索引复核均成功，不跳过镜像测试。

### 1.1 Maven 缓存与下载限流

使用 Maven 共享缓存的 CI 工作流先调用同一提交中的 `maven-dependencies.yml`，依赖准备成功后才启动原有测试任务。准备失败会使原有检查明确失败，不会把依赖失败变成成功的跳过状态。J09 完整交付继续使用上述专用仓库，保持 SNAPSHOT 隔离。

缓存键同时覆盖所有 POM、依赖锁、Maven Wrapper、公共仓库设置、下载参数和准备脚本。只有准备任务先完成整个 reactor 的外部依赖、插件及动态制品下载后，才保存这一版本的缓存；测试任务使用相同键恢复，并设置 `cache-read-only: true`，防止缓存服务故障时由只执行部分构建的消费者写入不完整缓存。新键避免继续复用旧的、不完整且无法追加保存的精确命中缓存。准备过程不安装本工程的 SNAPSHOT 制品。

`scripts/dev/prepare-maven-dependencies.py` 除了执行锁定插件的 `go-offline`，还从已有依赖锁补齐动态下载的 protoc、gRPC 代码生成器、配置解析库和隔离 HBase 进程依赖，并核验全部显式制品 SHA-256。JUnit provider 和 Platform launcher 分别跟随已锁定的 Surefire/Failsafe 及 JUnit 6 版本补齐。普通依赖和插件预解析本身不能证明这些执行期依赖已经缓存。准备阶段还通过 HBase reactor 的 `test-compile` 解析实际测试类路径（不执行测试），并在保存前离线重跑整个 reactor 的依赖解析。若 Resolver 明确列出缺少的普通传递制品，只针对这些坐标执行 `dependency:get`，最多补全三轮后重新离线验收；其他错误或仍未齐全均拒绝保存，不重试测试或整个构建生命周期。完整测试继续由原有质量任务执行。

`.mvn/maven.config` 将普通制品下载并发降为 2；缓存准备进一步降为 1。Resolver 对 429/503 等传输失败最多重试 4 次，无服务端等待提示时从 15 秒开始递增等待，间隔上限 60 秒，并保留其 `Retry-After` 处理。不会重新执行失败的测试来制造通过。跨工作流的冷缓存准备仍可能并发，GitHub 托管 runner 的出口及 Central 的服务端策略也不受本项目控制，因此这些措施不承诺永久消除 429。

J09 另将 Maven Wrapper 发行包缓存到 `~/.m2/wrapper/dists`，缓存键绑定 runner OS 和 `.mvn/wrapper/maven-wrapper.properties` 摘要；这与 Maven 制品仓库缓存分开，且 CI Action pin 放在 `build/ci-action-lock.json`，不改变 Maven 依赖缓存键。Wrapper 启动最多尝试 4 次，仅对明确记录的 HTTP 408、425、429、500、502、503、504 退避重试 2/4/8 秒；依赖解析、构建和测试失败仍立即按原流程失败。J09 job 失败时上传有限诊断文件，保留 7 天。

`python3 scripts/verify/test_maven_resilience.py` 使用锁定 Maven 和本地 HTTP 制品仓库验证 Maven Resolver 连续 429 后恢复、持续 429 的终止边界及 404 不重试；还通过实际 Maven Wrapper 和本地 HTTP 429/404 响应验证 Wrapper 的有界重试、硬错误快速失败与启动日志留存，并检查 Action pin、缓存键和准备失败门禁。它需要 `ETCD_HBASE_JDK21_HOME` 或 `JAVA_HOME` 指向锁定 JDK，不访问真实业务数据。

若后续接入 Nexus/Artifactory，需要先提供 GitHub CI 可访问的实际 HTTPS 代理服务，并明确访问方式和凭据 secret 名称。当前公开验证仍强制 Central 且不使用私有凭据；代理服务尚未配置或部署，接入时需同步维护对应构建路径和策略校验。

## 2 运行包与镜像边界

每次运行的 `runtime/` 是可搬移运行目录。`etcd-hbase` 启动脚本保留 Maven runtime classpath 顺序；运行 JAR 必须与本轮构建的七个生产模块逐一匹配。`testkit`、test JAR、`target/classes`、旧 protobuf 2.5.0、同名碰撞或过期 SNAPSHOT 不能进入运行 classpath。

```bash
JAVA_HOME="$ETCD_HBASE_JDK21_HOME" .artifacts/m0/public/<run>/runtime/etcd-hbase
```

默认开发配置绑定 loopback、使用动态端口，明确禁止把无认证模式作为公网服务。容器配置只把端口改为 2379，仍绑定 loopback；验证程序在容器网络命名空间内执行探针，不通过开放无认证端口完成测试。

镜像来源、tag、index/platform digest、平台与 JRE vendor/patch 记录于 [dependency-lock.json](../../build/dependency-lock.json)。Dockerfile 固定 `linux/amd64` 平台 manifest。镜像只复制已验证运行目录；基础镜像拉取需要访问公开 registry；`--network none` 限制构建容器的网络，不从镜像内执行 Maven 或下载项目依赖。测试容器使用 UID/GID 65532、只读根文件系统、有限 tmpfs/内存/进程数、无外部网络及 capabilities，测试结束在 30 秒总预算内删除本次 UUID 标记的资源；外层取消宽限为 45 秒，给内层 JVM/容器清理留出时间。镜像验证不执行 push 或部署。

## 3 许可、发布与验收

[release-decisions.json](../../build/release-decisions.json) 记录 I07/I08 的显式状态。当前项目许可证、完整第三方通知审阅、公开 artifact 坐标、发布位置与授权规则仍待维护者决定；本地 SNAPSHOT 不构成发布决定。`release-readiness.py --require-decisions` 在这些输入缺失时退出 `2`，普通检查只报告状态，不授权或执行发布。

运行目录保留原始依赖 JAR 中的 LICENSE/NOTICE，并在 `runtime-dependencies.json` 中记录逐 JAR 摘要及保留条目。该清单不是完整 SBOM 或许可审阅结论；根 LICENSE/正式 NOTICE 与完整发行审阅需待 I07。第三方来源规则见 [公开策略](open-source-policy.md) 和 [跟踪清单](../../THIRD_PARTY_NOTICES.md)。

B01/B02/B03/B06、I07/I08、正式评审与后续业务任务分别保留状态。公开构建通过不会将 M0 阶段标记为验收通过。原始运行报告及个人执行恢复记录留在本地，不随产品 PR 上传；GitHub 中写明检查范围、提交、结论和必要限制即可。
