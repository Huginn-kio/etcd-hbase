package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Shared canonical encoding and SHA-256 helpers for immutable manifest records. */
final class ManifestDigest {
  @FunctionalInterface
  interface Encoder {
    void write(DataOutputStream output) throws IOException;
  }

  private ManifestDigest() {}

  static byte[] encode(Encoder encoder) {
    try {
      var bytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(bytes)) {
        encoder.write(output);
      }
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory manifest encoding failed", impossible);
    }
  }

  static ByteKey sha256(Encoder encoder) {
    return sha256(encode(encoder));
  }

  static ByteKey sha256(byte[] value) {
    try {
      return ByteKey.copyOf(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
    output.writeInt(value.length);
    output.write(value);
  }
}
