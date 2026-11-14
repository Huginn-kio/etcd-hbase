package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

class TestTlsCertificateTest {
  @TempDir Path temporaryDirectory;

  @Test
  void forcedTerminationReapsProcessAndPreservesInterruptStatus() throws Exception {
    Process process = new ProcessBuilder("sh", "-c", "exec sleep 60").start();
    try {
      Thread.currentThread().interrupt();

      TestTlsCertificate.terminateAndReap(process);

      assertFalse(process.isAlive());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
      if (process.isAlive()) {
        process.destroyForcibly();
        process.waitFor();
      }
    }
  }

  @Test
  void createsCertificatesWithNoAndMultipleDnsNames() throws Exception {
    try (TestTlsCertificate withoutDns = TestTlsCertificate.createWithoutDnsSan(
            temporaryDirectory.resolve("without-dns"), "owner-a");
         TestTlsCertificate multipleDns = TestTlsCertificate.createWithDnsSans(
             temporaryDirectory.resolve("multiple-dns"), "multiple", List.of("owner-a", "owner-b"))) {
      assertNull(readCertificate(withoutDns.certificate()).getSubjectAlternativeNames());
      Collection<List<?>> names = readCertificate(multipleDns.certificate()).getSubjectAlternativeNames();
      assertEquals(2, names.stream().filter(name -> Integer.valueOf(2).equals(name.getFirst())).count());
    }
  }

  private static X509Certificate readCertificate(Path path) throws Exception {
    try (var input = Files.newInputStream(path)) {
      return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input);
    }
  }
}
