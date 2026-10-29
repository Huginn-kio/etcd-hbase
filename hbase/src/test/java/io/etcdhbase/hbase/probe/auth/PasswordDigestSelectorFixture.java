package io.etcdhbase.hbase.probe.auth;

import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.AuthenticationProviderSelector;
import org.apache.hadoop.hbase.security.provider.BuiltInProviderSelector;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/** Routes only the explicit fixture token kind to the account-password mechanism. */
public final class PasswordDigestSelectorFixture implements AuthenticationProviderSelector {
  private final BuiltInProviderSelector builtIn = new BuiltInProviderSelector();
  private SaslClientAuthenticationProvider passwordDigest;

  @Override
  public void configure(
      Configuration configuration, Collection<SaslClientAuthenticationProvider> providers) {
    passwordDigest =
        providers.stream()
            .filter(PasswordDigestSaslClientFixture.class::isInstance)
            .findFirst()
            .orElseThrow();
    builtIn.configure(
        configuration, providers.stream().filter(provider -> provider != passwordDigest).toList());
  }

  @Override
  public Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> selectProvider(
      String clusterId, User user) {
    for (var token : user.getTokens()) {
      if (PasswordDigestSaslClientFixture.TOKEN_KIND.equals(token.getKind().toString())
          && clusterId.equals(token.getService().toString())) {
        return new Pair<>(passwordDigest, token);
      }
    }
    return builtIn.selectProvider(clusterId, user);
  }
}
