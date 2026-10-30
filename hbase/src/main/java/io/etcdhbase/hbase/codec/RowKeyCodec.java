package io.etcdhbase.hbase.codec;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.spi.salt.SaltProfile;
import io.etcdhbase.spi.salt.SaltStrategy;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.hadoop.hbase.HConstants;

/** A single table-bound KV format: salt || escaped-key || optional BE64 revision. */
public final class RowKeyCodec {
  public static final String FORMAT_ID = "ordered-zero-escape-v1";
  public enum Shape { CURRENT, HISTORY }
  public record DecodedRow(ByteKey key, OptionalLong revision) {}

  private final SaltStrategy salt;
  private final Shape shape;

  public RowKeyCodec(SaltStrategy salt, Shape shape) {
    this.salt = Objects.requireNonNull(salt, "salt");
    this.shape = Objects.requireNonNull(shape, "shape");
  }

  public SaltProfile saltProfile() {
    return salt.profile();
  }

  public Shape shape() {
    return shape;
  }

  public int bucket(ByteKey key) {
    byte[] prefix = checkedSalt(key);
    long bucketNumber = 0;
    for (byte part : prefix) bucketNumber = (bucketNumber << 8) | Byte.toUnsignedInt(part);
    if (bucketNumber >= salt.profile().bucketCount()) {
      throw new IllegalArgumentException("salt provider returned an unknown bucket");
    }
    return (int) bucketNumber;
  }

  public byte[] encode(ByteKey key, long revision) {
    RevisionTimestamp.requireRevision(revision);
    byte[] encodedKey = encodeKey(key);
    int suffixLength = shape == Shape.HISTORY ? Long.BYTES : 0;
    byte[] prefix = checkedSalt(key);
    byte[] row = new byte[prefix.length + encodedKey.length + suffixLength];
    System.arraycopy(prefix, 0, row, 0, prefix.length);
    System.arraycopy(encodedKey, 0, row, prefix.length, encodedKey.length);
    if (shape == Shape.HISTORY) ByteBuffer.wrap(row, row.length - Long.BYTES, Long.BYTES).putLong(revision);
    return row;
  }

  public DecodedRow decode(byte[] row) {
    Objects.requireNonNull(row, "row");
    int prefixLength = salt.profile().saltWidthBytes();
    if (row.length > HConstants.MAX_ROW_LENGTH || row.length < prefixLength + 2) {
      throw new IllegalArgumentException("invalid physical row length");
    }
    DecodedKey decoded = decodeKey(row, prefixLength);
    int remaining = row.length - decoded.nextOffset();
    int expected = shape == Shape.HISTORY ? Long.BYTES : 0;
    if (remaining != expected) throw new IllegalArgumentException("invalid row suffix length");
    byte[] prefix = checkedSalt(decoded.key());
    if (!Arrays.equals(prefix, Arrays.copyOf(row, prefixLength))) {
      throw new IllegalArgumentException("row does not belong to its logical key bucket");
    }
    if (shape == Shape.CURRENT) return new DecodedRow(decoded.key(), OptionalLong.empty());
    long revision = ByteBuffer.wrap(row, decoded.nextOffset(), Long.BYTES).getLong();
    RevisionTimestamp.requireRevision(revision);
    return new DecodedRow(decoded.key(), OptionalLong.of(revision));
  }

  public KeyRange physicalRange(int bucketNumber, KeyRange logical) {
    byte[] prefix = salt.profile().encodeBucket(bucketNumber);
    if (logical.isEmpty()) return KeyRange.empty();
    if (logical.kind() == KeyRange.Kind.POINT) {
      return KeyRange.prefix(physicalBound(prefix, logical.lowerInclusive().orElseThrow()));
    }
    ByteKey start = logical.lowerInclusive().map(key -> physicalBound(prefix, key))
        .orElseGet(() -> ByteKey.copyOf(prefix));
    Optional<ByteKey> stop = logical.upperExclusive().map(key -> physicalBound(prefix, key));
    if (stop.isEmpty()) stop = KeyRange.prefixEnd(ByteKey.copyOf(prefix));
    // Positive infinity is valid only for the final all-ff bucket in this dedicated KV table.
    return KeyRange.interval(Optional.of(start), stop);
  }

  private byte[] checkedSalt(ByteKey key) {
    byte[] prefix = Objects.requireNonNull(salt.salt(key.bytes()), "salt prefix");
    if (prefix.length != salt.profile().saltWidthBytes()) {
      throw new IllegalArgumentException("salt provider returned an invalid prefix width");
    }
    return prefix;
  }

  private ByteKey physicalBound(byte[] prefix, ByteKey key) {
    byte[] encoded = encodeKey(key);
    byte[] bound = Arrays.copyOf(prefix, prefix.length + encoded.length);
    System.arraycopy(encoded, 0, bound, prefix.length, encoded.length);
    return ByteKey.copyOf(bound);
  }

  private byte[] encodeKey(ByteKey key) {
    byte[] original = key.bytes();
    long length = (long) original.length + 2;
    for (byte part : original) if (part == 0) length++;
    int overhead = salt.profile().saltWidthBytes() + (shape == Shape.HISTORY ? Long.BYTES : 0);
    if (length + overhead > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("encoded key exceeds the storage row limit");
    }
    byte[] encoded = new byte[(int) length];
    int offset = 0;
    for (byte part : original) {
      encoded[offset++] = part;
      if (part == 0) encoded[offset++] = (byte) 0xff;
    }
    return encoded;
  }

  private static DecodedKey decodeKey(byte[] row, int start) {
    var decoded = new ByteArrayOutputStream(row.length - start);
    int offset = start;
    while (offset < row.length) {
      byte part = row[offset++];
      if (part != 0) {
        decoded.write(part);
        continue;
      }
      if (offset == row.length) break;
      int escaped = Byte.toUnsignedInt(row[offset++]);
      if (escaped == 0) return new DecodedKey(ByteKey.copyOf(decoded.toByteArray()), offset);
      if (escaped != 0xff) throw new IllegalArgumentException("invalid ordered key escape");
      decoded.write(0);
    }
    throw new IllegalArgumentException("missing ordered key terminator");
  }

  private record DecodedKey(ByteKey key, int nextOffset) {}
}
