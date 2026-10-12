# M0 Java 工具链与公开依赖基线

本文件固定可公开获取的工程工具和依赖版本，供 Java 构建及后续 M0 原型复用。机器可读版本与归档摘要见 [`build/dependency-lock.json`](../../build/dependency-lock.json)。目标 HBase 版本尚未由 B02 确认，因此公开候选仅支持编译和依赖检查，不作为目标集群运行兼容性结论。

## Java 与 Maven

| 项目 | 固定值 | 用途 |
|---|---|---|
| JDK | Eclipse Temurin 21.0.12.1+1 | Java 21 LTS；不开启 preview |
| Maven | 3.9.16 | Wrapper 下载并校验发行归档 |
| Maven Wrapper | 3.3.4 | 项目级构建入口 |
| Java release | 21 | 编译目标；toolchain 选择同一主版本 JDK |

JDK 和 Maven 的平台归档 SHA-256、Maven Wrapper JAR SHA-256 均记录在锁文件中。构建通过 `ETCD_HBASE_JDK21_HOME` 指向已安装的 JDK 21；`scripts/dev/configure-toolchain.py` 只生成仓库本地的 ignored toolchain 文件。脚本不得改写用户级 `~/.m2/toolchains.xml`。公开依赖解析只使用 Maven Central 的 HTTPS 端点。

```sh
scripts/dev/configure-toolchain.py --java-home "$ETCD_HBASE_JDK21_HOME"
scripts/dev/mvn21.sh -version
scripts/dev/mvn21.sh clean verify
```

Maven 运行进程和 Maven compiler/test toolchain 都检查 Java 21。版本范围、`LATEST`、SNAPSHOT 插件或运行依赖、preview flags 均不允许。新增或升级依赖时，同步更新锁文件、来源、SHA-256 和选择理由，并在 Java 21 下重跑 reactor 验证。

## 插件和运行库

Maven 插件、gRPC、Protobuf/protoc、Netty、Guava、SLF4J、Hadoop、HBase、JSON/YAML 与测试库采用锁文件中的精确版本。gRPC 与 Protobuf 使用各自同一发布线的版本；protoc 及 protoc-gen-grpc-java 由 Maven 插件按平台 classifier 获取。默认服务传输使用 `grpc-netty-shaded`，隔离 gRPC 与 Hadoop/HBase 的 Netty 类路径。

Go testkit 的编译器和 etcd client module 版本也记录在锁文件中；对应 Go 源码和互通检查属于后续工作包，本层不声明已验证 Go/Java 互通。

## HBase 候选与目标兼容性

公开依赖候选为 `org.apache.hbase:hbase-shaded-client-byo-hadoop:2.6.6-hadoop3`，配套 Hadoop `3.4.3`。该候选来自 Apache HBase 2.6.6 的 Hadoop 3 构建，只用于依赖解析与编译检查。Apache HBase 支持矩阵将 Java 21 支持标记为 HBase 2.6.8+ 和 3.0.1+；公开下载页当前列出的 HBase 2.6/3.0 GA 版本尚未达到这些门槛。因此，本候选**未获官方 Java 21 运行支持**，不得据此宣称 Java 21 到 HBase 的运行兼容。

目标 HBase/Hadoop 发行版、认证模式和运行约束由 B02 提供后再确定目标依赖组合。目标输入未到位前，不把公开候选作为目标环境的替代，不运行或宣称目标 HBase 验收。

## 官方来源

- [Eclipse Temurin 21 发布页](https://adoptium.net/temurin/releases?version=21)
- [Apache Maven 3.9.16 发布说明](https://maven.apache.org/docs/3.9.16/release-notes.html)
- [Apache Maven Wrapper 3.3.4 与 distributionSha256Sum](https://maven.apache.org/tools/wrapper/maven-wrapper-plugin/wrapper-mojo.html)
- [Maven 官方插件列表](https://maven.apache.org/plugins/index.html)
- [gRPC Java 官方依赖与生成配置](https://github.com/grpc/grpc-java)
- [Protobuf 版本支持与 protoc/Java 版本对应关系](https://protobuf.dev/support/version-support/)
- [Apache HBase Java 支持矩阵](https://hbase.apache.org/docs/configuration/basic-prerequisites/)
- [Apache HBase 发布下载页](https://hbase.apache.org/downloads/)
- [Go 官方发布历史](https://go.dev/doc/devel/release)
