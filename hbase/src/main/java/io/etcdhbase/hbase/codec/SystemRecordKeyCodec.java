package io.etcdhbase.hbase.codec;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.spi.salt.SaltStrategy;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import org.apache.hadoop.hbase.HConstants;

/** Encodes candidate rows as bucket || kind || txnId || logical key for bounded ordered scans. */
public final class SystemRecordKeyCodec {
  public static final int FORMAT_VERSION = 2;

  public record DecodedKey(SystemRecordType type, ByteKey identity) {}

  private final SaltStrategy salt;

  public SystemRecordKeyCodec(SaltStrategy salt) {
    this.salt = Objects.requireNonNull(salt, "salt");
  }

  public int formatVersion() {
    return FORMAT_VERSION;
  }

  public byte[] encode(SystemRecordType type, ByteKey identity) {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(identity, "identity");
    byte[] identifier = identity.bytes();
    byte[] routingIdentifier = routingIdentifier(type, identifier);
    byte[] candidateKey = type == SystemRecordType.CANDIDATE
        ? encodeCandidateKey(routingIdentifier) : null;
    long identityBytes = type == SystemRecordType.CANDIDATE
        ? 16L + candidateKey.length : Integer.BYTES + (long) identifier.length;
    long rowLength = (long) salt.profile().saltWidthBytes() + 1 + identityBytes;
    if (identifier.length == 0 || rowLength > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("invalid or oversized system record identity");
    }
    byte[] prefix = prefix(type, identifier);
    ByteBuffer row = ByteBuffer.allocate((int) rowLength);
    row.put(prefix).put((byte) type.id());
    if (type == SystemRecordType.CANDIDATE) {
      row.put(identifier, 0, 16).put(candidateKey);
    } else {
      row.putInt(identifier.length).put(identifier);
    }
    return row.array();
  }

  public DecodedKey decode(byte[] row) {
    Objects.requireNonNull(row, "row");
    int saltWidth = salt.profile().saltWidthBytes();
    if (row.length <= saltWidth + 1 || row.length > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("invalid system record row length");
    }
    ByteBuffer encoded = ByteBuffer.wrap(row);
    byte[] prefix = new byte[saltWidth];
    encoded.get(prefix);
    SystemRecordType type = fromId(Byte.toUnsignedInt(encoded.get()));
    byte[] identity;
    if (type == SystemRecordType.CANDIDATE) {
      if (encoded.remaining() <= 16) {
        throw new IllegalArgumentException("candidate system identity has no logical key");
      }
      byte[] transaction = new byte[16];
      encoded.get(transaction);
      byte[] keyEncoding = new byte[encoded.remaining()];
      encoded.get(keyEncoding);
      byte[] key = decodeCandidateKey(keyEncoding);
      identity = ByteBuffer.allocate(16 + Integer.BYTES + key.length)
          .put(transaction).putInt(key.length).put(key).array();
    } else {
      if (encoded.remaining() < Integer.BYTES) {
        throw new IllegalArgumentException("system record identity length is missing");
      }
      int identityLength = encoded.getInt();
      if (identityLength <= 0 || identityLength != encoded.remaining()) {
        throw new IllegalArgumentException("invalid system record identity length");
      }
      identity = new byte[identityLength];
      encoded.get(identity);
    }
    checkedPrefix(prefix);
    if (!Arrays.equals(prefix, prefix(type, identity))) {
      throw new IllegalArgumentException("system record row has an invalid salt prefix");
    }
    return new DecodedKey(type, ByteKey.copyOf(identity));
  }

  /** Returns the configured bucket after validating the exact physical system-row identity. */
  public int bucket(SystemRecordType type, ByteKey identity) {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(identity, "identity");
    byte[] bytes = identity.bytes();
    byte[] routingIdentifier = routingIdentifier(type, bytes);
    byte[] candidateKey = type == SystemRecordType.CANDIDATE
        ? encodeCandidateKey(routingIdentifier) : null;
    long identityBytes = type == SystemRecordType.CANDIDATE
        ? 16L + candidateKey.length : Integer.BYTES + (long) bytes.length;
    long rowLength = (long) salt.profile().saltWidthBytes() + 1 + identityBytes;
    if (bytes.length == 0 || rowLength > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("invalid or oversized system record identity");
    }
    return bucketNumber(salt.salt(saltIdentifier(type, bytes, routingIdentifier)));
  }

  private byte[] prefix(SystemRecordType type, byte[] identity) {
    byte[] routingIdentifier = routingIdentifier(type, identity);
    return checkedPrefix(salt.salt(saltIdentifier(type, identity, routingIdentifier)));
  }

  private static byte[] saltIdentifier(
      SystemRecordType type, byte[] identity, byte[] routingIdentifier) {
    return type == SystemRecordType.CANDIDATE ? routingIdentifier : route(type, identity);
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

  private byte[] checkedPrefix(byte[] prefix) {
    if (prefix == null || prefix.length != salt.profile().saltWidthBytes()) {
      throw new IllegalArgumentException("salt provider returned an invalid system prefix");
    }
    if (bucketNumberUnchecked(prefix) >= salt.profile().bucketCount()) {
      throw new IllegalArgumentException("salt provider returned an unknown system bucket");
    }
    return prefix;
  }

  private int bucketNumber(byte[] prefix) {
    return (int) bucketNumberUnchecked(checkedPrefix(prefix));
  }

  private long bucketNumberUnchecked(byte[] prefix) {
    long bucket = 0;
    for (byte part : prefix) bucket = (bucket << Byte.SIZE) | Byte.toUnsignedInt(part);
    return bucket;
  }

  private static byte[] routingIdentifier(SystemRecordType type, byte[] identity) {
    if (type != SystemRecordType.CANDIDATE) return identity;
    if (identity.length <= 16 + Integer.BYTES) {
      throw new IllegalArgumentException("candidate identity is missing its logical key");
    }
    int keyLength = ByteBuffer.wrap(identity, 16, Integer.BYTES).getInt();
    if (keyLength <= 0 || keyLength != identity.length - 16 - Integer.BYTES) {
      throw new IllegalArgumentException("candidate identity has an invalid logical key length");
    }
    return Arrays.copyOfRange(identity, 16 + Integer.BYTES, identity.length);
  }

  private byte[] encodeCandidateKey(byte[] key) {
    if (key.length == 0) throw new IllegalArgumentException("candidate logical key must not be empty");
    long encodedLength = (long) key.length + 2;
    for (byte part : key) if (part == 0) encodedLength++;
    if (salt.profile().saltWidthBytes() + 1L + 16 + encodedLength > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("candidate key exceeds the system row limit");
    }
    byte[] encoded = new byte[Math.toIntExact(encodedLength)];
    int offset = 0;
    for (byte part : key) {
      encoded[offset++] = part;
      if (part == 0) encoded[offset++] = (byte) 0xff;
    }
    encoded[offset++] = 0;
    encoded[offset] = 0;
    return encoded;
  }

  private static byte[] decodeCandidateKey(byte[] encoded) {
    var decoded = new ByteArrayOutputStream(encoded.length);
    for (int offset = 0; offset < encoded.length; ) {
      byte part = encoded[offset++];
      if (part != 0) {
        decoded.write(part);
        continue;
      }
      if (offset == encoded.length) {
        throw new IllegalArgumentException("candidate key is missing its ordered terminator");
      }
      int escaped = Byte.toUnsignedInt(encoded[offset++]);
      if (escaped == 0) {
        if (offset != encoded.length || decoded.size() == 0) {
          throw new IllegalArgumentException("candidate key terminator is misplaced");
        }
        return decoded.toByteArray();
      }
      if (escaped != 0xff) {
        throw new IllegalArgumentException("candidate key contains an invalid zero escape");
      }
      decoded.write(0);
    }
    throw new IllegalArgumentException("candidate key is missing its ordered terminator");
  }
}
