package io.etcdhbase.hbase.probe.auth;

import org.apache.hadoop.hbase.security.provider.DigestSaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslAuthMethod;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

/** Test extension with its own method code; it never replaces HBase's delegation-token provider. */
public final class PasswordDigestSaslClientFixture extends DigestSaslClientAuthenticationProvider {
  public static final String TOKEN_KIND = "ETCDHBASE_TEST_PASSWORD_DIGEST";
  static final SaslAuthMethod METHOD =
      new SaslAuthMethod(
          "TEST_PASSWORD_DIGEST", (byte) 93, "DIGEST-MD5", AuthenticationMethod.TOKEN);

  @Override
  public SaslAuthMethod getSaslAuthMethod() {
    return METHOD;
  }

  @Override
  public String getTokenKind() {
    return TOKEN_KIND;
  }
}
