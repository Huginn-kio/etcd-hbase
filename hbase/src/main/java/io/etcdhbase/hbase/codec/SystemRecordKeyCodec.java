package io.etcdhbase.hbase.codec;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.spi.salt.SaltStrategy;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import org.apache.hadoop.hbase.HConstants;

/** Encodes point-addressable M2 system rows as bucket || kind || length || identity. */
public final class SystemRecordKeyCodec {
  public record DecodedKey(SystemRecordType type, ByteKey identity) {}

  private final SaltStrategy salt;

  public SystemRecordKeyCodec(SaltStrategy salt) {
    this.salt = Objects.requireNonNull(salt, "salt");
  }

  public byte[] encode(SystemRecordType type, ByteKey identity) {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(identity, "identity");
    byte[] identifier = identity.bytes();
    long rowLength = (long) salt.profile().saltWidthBytes() + 1 + Integer.BYTES + identifier.length;
    if (identifier.length == 0 || rowLength > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("invalid or oversized system record identity");
    }
    byte[] route = route(type, identifier);
    byte[] prefix = salt.salt(route);
    requireValidPrefix(prefix);
    ByteBuffer row = ByteBuffer.allocate((int) rowLength);
    row.put(prefix).put((byte) type.id()).putInt(identifier.length).put(identifier);
    return row.array();
  }

  public DecodedKey decode(byte[] row) {
    Objects.requireNonNull(row, "row");
    int saltWidth = salt.profile().saltWidthBytes();
    if (row.length <= saltWidth + 1 + Integer.BYTES || row.length > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("invalid system record row length");
    }
    ByteBuffer encoded = ByteBuffer.wrap(row);
    byte[] prefix = new byte[saltWidth];
    encoded.get(prefix);
    SystemRecordType type = fromId(Byte.toUnsignedInt(encoded.get()));
    int identityLength = encoded.getInt();
    if (identityLength <= 0 || identityLength != encoded.remaining()) {
      throw new IllegalArgumentException("invalid system record identity length");
    }
    byte[] identity = new byte[identityLength];
    encoded.get(identity);
    requireValidPrefix(prefix);
    if (!Arrays.equals(prefix, salt.salt(route(type, identity)))) {
      throw new IllegalArgumentException("system record row has an invalid salt prefix");
    }
    return new DecodedKey(type, ByteKey.copyOf(identity));
  }

  private static byte[] route(SystemRecordType type, byte[] identity) {
    return ByteBuffer.allocate(1 + Integer.BYTES + identity.length)
        .put((byte) type.id()).putInt(identity.length).put(identity).array();
  }

  private static SystemRecordType fromId(int id) {
    for (SystemRecordType type : SystemRecordType.values()) {
      if (type.id() == id) return type;
    }
    throw new IllegalArgumentException("unknown system record type");
  }

  private void requireValidPrefix(byte[] prefix) {
    if (prefix == null || prefix.length != salt.profile().saltWidthBytes()) {
      throw new IllegalArgumentException("salt provider returned an invalid system prefix");
    }
    long bucket = 0;
    for (byte part : prefix) bucket = (bucket << Byte.SIZE) | Byte.toUnsignedInt(part);
    if (bucket >= salt.profile().bucketCount()) {
      throw new IllegalArgumentException("salt provider returned an unknown system bucket");
    }
  }
}
