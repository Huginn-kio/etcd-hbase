package io.etcdhbase.hbase.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.etcdhbase.spi.identity.BackendCredentials;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.AuthenticationProviderSelector;
import org.apache.hadoop.hbase.security.provider.BuiltInProviderSelector;
import org.apache.hadoop.hbase.security.provider.DigestSaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslAuthMethod;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.jupiter.api.Test;

/** Verifies the public HBase provider extension path without changing the JVM's global registry. */
public class HBaseAuthenticationProvidersTest {
  @Test
  void officialRegistryLoadsCustomProviderAndSelectsItAlongsideBuiltInMechanisms()
      throws Exception {
    var configuration = new Configuration(false);
    configuration.set("hbase.security.authentication", "kerberos");
    configuration.setStrings(
        SaslClientAuthenticationProviders.EXTRA_PROVIDERS_KEY, FixtureProvider.class.getName());
    configuration.setClass(
        SaslClientAuthenticationProviders.SELECTOR_KEY,
        FixtureSelector.class,
        AuthenticationProviderSelector.class);
    // Use an independent registry for the fixture, never reset a live client's global registry.
    var registry = new SaslClientAuthenticationProviders(configuration);
    assertEquals(4, registry.getNumRegisteredProviders());
    assertEquals("SIMPLE", registry.getSimpleProvider().getFirst().getSaslAuthMethod().getName());
    try (var credentials = new BackendCredentials("alice", "fixture-password".toCharArray());
        var context = DigestPasswordUser.create(credentials, "cluster")) {
      var selected = registry.selectProvider("cluster", context.user());
      assertEquals("DIGEST", selected.getFirst().getSaslAuthMethod().getName());
      var token = context.user().getTokens().iterator().next();
      token.setKind(new Text(FixtureProvider.KIND));
      selected = registry.selectProvider("cluster", context.user());
      assertInstanceOf(FixtureProvider.class, selected.getFirst());
      assertSame(token, selected.getSecond());
      assertNull(registry.selectProvider("wrong-cluster", context.user()));
    }
  }

  public static final class FixtureProvider extends DigestSaslClientAuthenticationProvider {
    static final String KIND = "FIXTURE_CREDENTIAL";

    @Override
    public SaslAuthMethod getSaslAuthMethod() {
      return new SaslAuthMethod(
          "FIXTURE", (byte) 93, "DIGEST-MD5", UserGroupInformation.AuthenticationMethod.TOKEN);
    }

    @Override
    public String getTokenKind() {
      return KIND;
    }
  }

  public static final class FixtureSelector implements AuthenticationProviderSelector {
    private final BuiltInProviderSelector builtIn = new BuiltInProviderSelector();
    private SaslClientAuthenticationProvider custom;

    @Override
    public void configure(
        Configuration configuration, Collection<SaslClientAuthenticationProvider> providers) {
      custom =
          providers.stream().filter(FixtureProvider.class::isInstance).findFirst().orElseThrow();
      builtIn.configure(
          configuration, providers.stream().filter(provider -> provider != custom).toList());
    }

    @Override
    public Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> selectProvider(
        String clusterId, User user) {
      for (var token : user.getTokens()) {
        if (FixtureProvider.KIND.equals(token.getKind().toString())
            && clusterId.equals(token.getService().toString())) {
          return new Pair<>(custom, token);
        }
      }
      return builtIn.selectProvider(clusterId, user);
    }
  }
}
