package io.etcdhbase.hbase.probe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.hbase.identity.KerberosUserContext;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SecureHBaseAuthenticationIT {
  @Test
  @Timeout(300)
  void authenticatesAnActualSecureHBaseCluster() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.startSecure()) {
      var configuration = cluster.clientConfiguration();
      assertEquals("kerberos", configuration.get("hbase.security.authentication"));
      assertTrue(configuration.getBoolean("hbase.security.authorization", false));
      String principal = "admin@" + configuration.get(TemporaryKerberosSecurity.PREFIX + "realm");
      try (var user =
              KerberosUserContext.fromKeytab(
                  principal,
                  Path.of(configuration.get(TemporaryKerberosSecurity.PREFIX + "admin.keytab")));
          var connection =
              ConnectionFactory.createAsyncConnection(configuration, user.user())
                  .get(30, TimeUnit.SECONDS)) {
        assertTrue(
            connection.getAdmin().listNamespaces().get(30, TimeUnit.SECONDS).contains("default"));
      }
    }
  }
}
