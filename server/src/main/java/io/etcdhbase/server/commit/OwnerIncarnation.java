package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitOwnerService;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/** Encodes a stable routing identity and a fresh fencing identity within the control-row limit. */
final class OwnerIncarnation {
  private static final String PREFIX = "~";
  private static final String SEPARATOR = ":";

  private OwnerIncarnation() {}

  static String newId(String peerId) {
    return routeKey(peerId) + SEPARATOR + CommitOwnerService.newInstanceId();
  }

  static String routeKey(String peerId) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256")
          .digest(peerId.getBytes(StandardCharsets.UTF_8));
      return PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 unavailable", impossible);
    }
  }

  static String routeKeyFromInstance(String instance) {
    int separator = instance.indexOf(SEPARATOR);
    if (!instance.startsWith(PREFIX) || separator < 0) return null;
    String key = instance.substring(0, separator);
    String suffix = instance.substring(separator + 1);
    try {
      if (!java.util.UUID.fromString(suffix).toString().equals(suffix)) return null;
    } catch (IllegalArgumentException invalid) {
      return null;
    }
    return key;
  }
}
