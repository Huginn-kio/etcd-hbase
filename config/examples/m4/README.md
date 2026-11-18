# M4 临时验证配置

`activation.json` 只用于公开测试代码和 MiniCluster 验证。生产入口通过默认 `PRODUCTION` profile 加载配置；在 J51 身份生命周期能力完成前，只要 `m4_lease_api_enabled` 为 `true`，启动即会失败。

测试代码必须从可信测试装配显式构造 `M3RuntimeConfigurationLoader(ActivationProfile.PUBLIC_TEST)`。profile 由 loader 注入，不能通过配置文件声明或覆盖。普通 M3 配置继续默认关闭 Lease API。
