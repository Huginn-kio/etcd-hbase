package io.etcdhbase.hbase.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.spi.identity.BackendCredentials;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.provider.GssSaslClientAuthenticationProvider;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kerby.kerberos.kerb.client.KrbClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** A real temporary KDC and HBase GSSAPI client, not a target HBase authorization test. */
class KerberosUserContextTest {
  @TempDir Path temporary;

  @Test
  @Timeout(90)
  void authenticatesPasswordKeytabAndTicketCacheWithIsolatedSubjects() throws Exception {
    UserGroupInformation processUser = UserGroupInformation.getLoginUser();
    var properties = MiniKdc.createConf();
    properties.setProperty(MiniKdc.TRANSPORT, "TCP");
    var kdc = new MiniKdc(properties, temporary.resolve("kdc").toFile());
    kdc.start();
    try {
      String alice = "alice@" + kdc.getRealm();
      String bob = "bob@" + kdc.getRealm();
      String serverPrincipal = "hbase/localhost@" + kdc.getRealm();
      kdc.createPrincipal("alice", "fixture-password");
      Path keytab = temporary.resolve("bob.keytab");
      kdc.createPrincipal(keytab.toFile(), "bob");
      Path serverKeytab = temporary.resolve("hbase.keytab");
      kdc.createPrincipal(serverKeytab.toFile(), "hbase/localhost");

      try (var credentials = new BackendCredentials(alice, "fixture-password".toCharArray());
          var passwordUser = KerberosUserContext.fromPassword(credentials);
          var keytabUser = KerberosUserContext.fromKeytab(bob, keytab)) {
        credentials.close();
        assertEquals(alice, passwordUser.user().getName());
        assertEquals(bob, keytabUser.user().getName());
        assertTrue(passwordUser.user().getUGI().hasKerberosCredentials());
        assertTrue(keytabUser.user().getUGI().hasKerberosCredentials());
        assertNotSame(passwordUser.user().getUGI(), keytabUser.user().getUGI());
        exchangeWithOfficialHBaseClient(passwordUser, serverPrincipal, serverKeytab);

        var krbClient = new KrbClient(kdc.getKrb5conf().getParentFile());
        krbClient.setKdcHost(kdc.getHost());
        krbClient.setKdcRealm(kdc.getRealm());
        krbClient.setKdcTcpPort(kdc.getPort());
        krbClient.init();
        Path cache = temporary.resolve("alice.ccache");
        krbClient.storeTicket(krbClient.requestTgt(alice, "fixture-password"), cache.toFile());
        try (var cacheUser = KerberosUserContext.fromTicketCache(alice, cache)) {
          assertEquals(alice, cacheUser.user().getName());
          assertTrue(cacheUser.user().getUGI().hasKerberosCredentials());
        }
        assertThrows(IOException.class, () -> KerberosUserContext.fromTicketCache(bob, cache));
      }
      try (var wrong = new BackendCredentials(alice, "wrong-fixture-password".toCharArray())) {
        var failure =
            assertThrows(IOException.class, () -> KerberosUserContext.fromPassword(wrong));
        assertFalse(failure.getMessage().contains("wrong-fixture-password"));
      }
      assertSame(processUser, UserGroupInformation.getLoginUser());
    } finally {
      kdc.stop();
    }
  }

  @Test
  void rejectsMissingCredentialFilesAndPasswordlessOrUnqualifiedLogins() {
    assertThrows(
        IOException.class,
        () -> KerberosUserContext.fromKeytab("alice@EXAMPLE.TEST", temporary.resolve("missing")));
    try (var noPassword = new BackendCredentials("alice@EXAMPLE.TEST", new char[0]);
        var noRealm = new BackendCredentials("alice", new char[] {'x'})) {
      assertThrows(
          IllegalArgumentException.class, () -> KerberosUserContext.fromPassword(noPassword));
      assertThrows(IllegalArgumentException.class, () -> KerberosUserContext.fromPassword(noRealm));
    }
  }

  private static void exchangeWithOfficialHBaseClient(
      KerberosUserContext user, String principal, Path serverKeytab) throws Exception {
    var serverSubject = new Subject();
    Map<String, String> options =
        Map.of(
            "principal",
            principal,
            "useKeyTab",
            "true",
            "keyTab",
            serverKeytab.toString(),
            "storeKey",
            "true",
            "doNotPrompt",
            "true",
            "isInitiator",
            "false");
    var serverLogin =
        new LoginContext(
            "fixture",
            serverSubject,
            null,
            new javax.security.auth.login.Configuration() {
              @Override
              public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[] {
                  new AppConfigurationEntry(
                      "com.sun.security.auth.module.Krb5LoginModule",
                      AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                      options)
                };
              }
            });
    serverLogin.login();
    try {
      Map<String, String> properties = Map.of(Sasl.QOP, "auth");
      SaslServer server =
          Subject.callAs(
              serverSubject,
              () ->
                  Sasl.createSaslServer(
                      "GSSAPI",
                      "hbase",
                      "localhost",
                      properties,
                      callbacks -> {
                        for (var callback : callbacks) {
                          if (callback instanceof AuthorizeCallback authorization) {
                            authorization.setAuthorized(
                                authorization
                                    .getAuthenticationID()
                                    .equals(authorization.getAuthorizationID()));
                          } else {
                            throw new javax.security.auth.callback.UnsupportedCallbackException(
                                callback);
                          }
                        }
                      }));
      assertNotNull(server);
      try {
        user.user()
            .getUGI()
            .doAs(
                (PrivilegedExceptionAction<Void>)
                    () -> {
                      var client =
                          new GssSaslClientAuthenticationProvider()
                              .createClient(
                                  new Configuration(false),
                                  InetAddress.getLoopbackAddress(),
                                  principal,
                                  null,
                                  false,
                                  properties);
                      assertNotNull(client);
                      try {
                        byte[] challenge = new byte[0];
                        for (int step = 0;
                            step < 10 && !(client.isComplete() && server.isComplete());
                            step++) {
                          byte[] response = client.evaluateChallenge(challenge);
                          if (response != null) {
                            challenge =
                                Subject.callAs(
                                    serverSubject, () -> server.evaluateResponse(response));
                          }
                        }
                        assertTrue(client.isComplete());
                        assertTrue(server.isComplete());
                        assertEquals(user.user().getName(), server.getAuthorizationID());
                        return null;
                      } finally {
                        client.dispose();
                      }
                    });
      } finally {
        server.dispose();
      }
    } finally {
      serverLogin.logout();
    }
  }
}
