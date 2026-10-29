package io.etcdhbase.hbase.probe;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kerby.kerberos.kerb.client.KrbClient;

/** Test-child-only KDC and server login; never used by a production identity provider. */
final class TemporaryKerberosSecurity implements AutoCloseable {
  static final String PREFIX = "etcdhbase.test.security.";
  static final String PASSWORD = "temporary-fixture-password";
  private final MiniKdc kdc;

  private TemporaryKerberosSecurity(MiniKdc kdc) {
    this.kdc = kdc;
  }

  static TemporaryKerberosSecurity start(Configuration configuration, Path directory)
      throws Exception {
    Files.createDirectories(directory);
    var properties = MiniKdc.createConf();
    properties.setProperty(MiniKdc.TRANSPORT, "TCP");
    properties.setProperty(MiniKdc.KDC_BIND_ADDRESS, "127.0.0.1");
    var kdc = new MiniKdc(properties, directory.resolve("kdc").toFile());
    kdc.start();
    try {
      Path serviceKeytab = directory.resolve("service.keytab");
      Path adminKeytab = directory.resolve("admin.keytab");
      Path userKeytab = directory.resolve("keytab-user.keytab");
      kdc.createPrincipal(serviceKeytab.toFile(), "hbase/localhost");
      kdc.createPrincipal(adminKeytab.toFile(), "admin");
      kdc.createPrincipal(userKeytab.toFile(), "keytab-user");
      for (String principal : new String[] {"alice", "reader", "denied"}) {
        kdc.createPrincipal(principal, PASSWORD);
      }
      String servicePrincipal = "hbase/localhost@" + kdc.getRealm();
      configuration.set("hbase.security.authentication", "kerberos");
      configuration.set("hadoop.security.authentication", "kerberos");
      configuration.setBoolean("hbase.security.authorization", true);
      configuration.set("hbase.superuser", "hbase,admin");
      configuration.set("hbase.master.kerberos.principal", servicePrincipal);
      configuration.set("hbase.regionserver.kerberos.principal", servicePrincipal);
      configuration.set("hbase.master.keytab.file", serviceKeytab.toString());
      configuration.set("hbase.regionserver.keytab.file", serviceKeytab.toString());
      configuration.set("hbase.rpc.protection", "authentication");
      configuration.setBoolean("hbase.ipc.client.fallback-to-simple-auth-allowed", false);
      String accessController = "org.apache.hadoop.hbase.security.access.AccessController";
      configuration.set("hbase.coprocessor.master.classes", accessController);
      configuration.set("hbase.coprocessor.regionserver.classes", accessController);
      configuration.setStrings(
          "hbase.coprocessor.region.classes",
          accessController,
          "org.apache.hadoop.hbase.security.token.TokenProvider");
      configuration.setInt("hfile.format.version", 3);
      configuration.set(PREFIX + "realm", kdc.getRealm());
      configuration.set(PREFIX + "krb5", kdc.getKrb5conf().getAbsolutePath());
      configuration.set(PREFIX + "admin.keytab", adminKeytab.toString());
      configuration.set(PREFIX + "user.keytab", userKeytab.toString());
      var client = new KrbClient(kdc.getKrb5conf().getParentFile());
      client.setKdcHost(kdc.getHost());
      client.setKdcRealm(kdc.getRealm());
      client.setKdcTcpPort(kdc.getPort());
      client.init();
      Path cache = directory.resolve("alice.ccache");
      client.storeTicket(client.requestTgt("alice@" + kdc.getRealm(), PASSWORD), cache.toFile());
      configuration.set(PREFIX + "ticket.cache", cache.toString());
      // Global server login is confined to the disposable child process, not the client process.
      UserGroupInformation.setConfiguration(configuration);
      UserGroupInformation.loginUserFromKeytab(servicePrincipal, serviceKeytab.toString());
      return new TemporaryKerberosSecurity(kdc);
    } catch (Exception | Error failure) {
      kdc.stop();
      throw failure;
    }
  }

  @Override
  public void close() {
    kdc.stop();
  }
}
