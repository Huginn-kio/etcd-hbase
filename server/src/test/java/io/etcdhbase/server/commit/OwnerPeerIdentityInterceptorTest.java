package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.etcdhbase.server.TestTlsCertificate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OwnerPeerIdentityInterceptorTest {
  @TempDir Path temporaryDirectory;

  @Test
  void acceptsOnlyOneConfiguredDnsSan() throws Exception {
    var interceptor = new OwnerPeerIdentityInterceptor(List.of("owner-a", "owner-b"));
    try (TestTlsCertificate accepted = TestTlsCertificate.create(temporaryDirectory, "owner-a")) {
      assertEquals("owner-a", interceptor.identityFromCertificate(readCertificate(accepted.certificate())));
    }
  }

  @Test
  void rejectsUnregisteredDnsSanEvenWhenItsCertificateIsTrusted() throws Exception {
    var interceptor = new OwnerPeerIdentityInterceptor(List.of("owner-a", "owner-b"));
    try (TestTlsCertificate certificate = TestTlsCertificate.create(
        temporaryDirectory, "unregistered-peer")) {
      assertNull(interceptor.identityFromCertificate(readCertificate(certificate.certificate())));
    }
  }

  @Test
  void rejectsCertificatesWithoutDnsSanInsteadOfFallingBackToCommonName() throws Exception {
    var interceptor = new OwnerPeerIdentityInterceptor(List.of("owner-a"));
    try (TestTlsCertificate certificate = TestTlsCertificate.createWithoutDnsSan(
        temporaryDirectory, "owner-a")) {
      assertNull(interceptor.identityFromCertificate(readCertificate(certificate.certificate())));
    }
  }

  @Test
  void rejectsCertificatesWithMultipleDnsSansEvenWhenBothAreConfigured() throws Exception {
    var interceptor = new OwnerPeerIdentityInterceptor(List.of("owner-a", "owner-b"));
    try (TestTlsCertificate certificate = TestTlsCertificate.createWithDnsSans(
        temporaryDirectory, "ambiguous-owner", List.of("owner-a", "owner-b"))) {
      assertNull(interceptor.identityFromCertificate(readCertificate(certificate.certificate())));
    }
  }

  private static X509Certificate readCertificate(Path path) throws Exception {
    try (var input = Files.newInputStream(path)) {
      return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input);
    }
  }
}
