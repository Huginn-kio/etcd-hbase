package io.etcdhbase.server;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Creates an ephemeral certificate and private key for mTLS tests. */
public final class TestTlsCertificate implements AutoCloseable {
  private static final long GENERATION_TIMEOUT_SECONDS = 30;

  private final Path certificate;
  private final Path privateKey;

  private TestTlsCertificate(Path certificate, Path privateKey) {
    this.certificate = certificate;
    this.privateKey = privateKey;
  }

  public static TestTlsCertificate create(Path directory, String peerIdentity)
      throws IOException, InterruptedException {
    if (peerIdentity == null) throw new IllegalArgumentException("test certificate identity is invalid");
    return create(directory, peerIdentity, List.of(peerIdentity));
  }

  public static TestTlsCertificate createWithoutDnsSan(Path directory, String commonName)
      throws IOException, InterruptedException {
    return create(directory, commonName, List.of());
  }

  public static TestTlsCertificate createWithDnsSans(Path directory, String commonName,
      List<String> dnsNames) throws IOException, InterruptedException {
    return create(directory, commonName, dnsNames);
  }

  private static TestTlsCertificate create(Path directory, String commonName, List<String> dnsNames)
      throws IOException, InterruptedException {
    Objects.requireNonNull(directory, "directory");
    Objects.requireNonNull(dnsNames, "dnsNames");
    if (commonName == null || !commonName.matches("[a-z0-9][a-z0-9.-]{0,252}")
        || dnsNames.stream().anyMatch(name -> name == null
            || !name.matches("[a-z0-9][a-z0-9.-]{0,252}"))) {
      throw new IllegalArgumentException("test certificate identity is invalid");
    }
    Files.createDirectories(directory);
    Path certificate = directory.resolve(commonName + ".crt.pem");
    Path privateKey = directory.resolve(commonName + ".key.pem");
    boolean generated = false;
    Process process = null;
    try {
      var command = new ArrayList<>(List.of("openssl", "req", "-x509", "-newkey",
          "rsa:2048", "-nodes", "-keyout", privateKey.toString(), "-out",
          certificate.toString(), "-days", "2", "-subj", "/CN=" + commonName));
      if (!dnsNames.isEmpty()) {
        command.add("-addext");
        command.add("subjectAltName=" + String.join(",", dnsNames.stream()
            .map(name -> "DNS:" + name).toList()));
      }
      process = new ProcessBuilder(command)
          .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
      if (!process.waitFor(GENERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        process.waitFor();
        throw new IOException("OpenSSL did not finish generating the test certificate");
      }
      if (process.exitValue() != 0) {
        throw new IOException("OpenSSL could not generate the test certificate");
      }
      restrictPrivateKey(privateKey);
      generated = true;
      return new TestTlsCertificate(certificate, privateKey);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw interrupted;
    } finally {
      if (!generated) {
        terminateAndReap(process);
        deleteIfPresent(privateKey);
        deleteIfPresent(certificate);
      }
    }
  }

  public Path certificate() { return certificate; }

  public Path privateKey() { return privateKey; }

  public void delete() {
    IOException failure = null;
    try {
      Files.deleteIfExists(privateKey);
    } catch (IOException cleanupFailure) {
      failure = cleanupFailure;
    }
    try {
      Files.deleteIfExists(certificate);
    } catch (IOException cleanupFailure) {
      if (failure == null) failure = cleanupFailure;
      else failure.addSuppressed(cleanupFailure);
    }
    if (failure != null) throw new UncheckedIOException("could not delete test TLS files", failure);
  }

  @Override
  public void close() {
    delete();
  }

  private static void restrictPrivateKey(Path privateKey) throws IOException {
    try {
      Files.setPosixFilePermissions(privateKey,
          EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    } catch (UnsupportedOperationException ignored) {
      // The test directory is private on platforms without POSIX file permissions.
    }
  }

  private static void deleteIfPresent(Path path) throws IOException {
    Files.deleteIfExists(path);
  }

  static void terminateAndReap(Process process) {
    if (process == null || !process.isAlive()) return;
    boolean restoreInterrupt = Thread.interrupted();
    process.destroyForcibly();
    try {
      while (process.isAlive()) {
        try {
          process.waitFor();
        } catch (InterruptedException interrupted) {
          restoreInterrupt = true;
          process.destroyForcibly();
        }
      }
    } finally {
      if (restoreInterrupt) Thread.currentThread().interrupt();
    }
  }
}
