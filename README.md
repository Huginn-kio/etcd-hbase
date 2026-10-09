# etcd-hbase

An etcd v3 API-compatible server backed by Apache HBase, built with Java 21.

面向开源的独立服务。默认发行版提供公开可用的实现，扩展发行版通过公开 SPI 复用相同内核。

## 开发与评审

- [开发计划与进度记录](DEVELOPMENT_PLAN.md)：架构与扩展边界、9 个阶段、78 个任务、完成标准、风险与评审记录。

当前为规划阶段，尚未交付可运行实现。首版采用轻量 Count，兼容范围与差异以开发计划的能力矩阵为准，不声明完整等价于原生 etcd。
