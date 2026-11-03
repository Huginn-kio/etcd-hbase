package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.hbase.codec.SystemRecordKeyCodec;
import io.etcdhbase.spi.salt.SaltStrategy;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;

/** Validated binding to the pre-provisioned system table; it never creates or adopts a table. */
public final class SystemTableBinding {
  private final TableName table;
  private final byte[] family;
  private final String tableGroupId;
  private final String saltProfileId;
  private final SystemRecordKeyCodec keyCodec;
  private final ByteKey profileFingerprint;

  public SystemTableBinding(TableName table, byte[] family, String tableGroupId,
      String saltProfileId, SaltStrategy salt) {
    this.table = Objects.requireNonNull(table, "table");
    this.family = Objects.requireNonNull(family, "family").clone();
    this.tableGroupId = requireIdentifier(tableGroupId, "tableGroupId");
    this.saltProfileId = requireIdentifier(saltProfileId, "saltProfileId");
    SaltStrategy checkedSalt = Objects.requireNonNull(salt, "salt");
    this.keyCodec = new SystemRecordKeyCodec(checkedSalt);
    this.profileFingerprint = ByteKey.copyOf(fingerprint(table, this.family, this.tableGroupId,
        this.saltProfileId, checkedSalt.profile()));
    if (this.family.length == 0) throw new IllegalArgumentException("system family must not be empty");
  }

  public TableName table() { return table; }
  public byte[] family() { return family.clone(); }
  public String tableGroupId() { return tableGroupId; }
  public SystemRecordKeyCodec keyCodec() { return keyCodec; }
  public ByteKey profileFingerprint() { return profileFingerprint; }

  public byte[] controlRow() {
    return keyCodec.encode(SystemRecordType.CONTROL,
        ByteKey.copyOf(tableGroupId.getBytes(StandardCharsets.UTF_8)));
  }

  public byte[] row(SystemRecordType type, ByteKey identity) {
    return keyCodec.encode(type, identity);
  }

  /** Requires the exact pre-provisioned M0 table identity and a durable, non-expiring family. */
  public void validate(TableDescriptor descriptor) {
    Objects.requireNonNull(descriptor, "descriptor");
    if (!descriptor.getTableName().equals(table)
        || descriptor.getColumnFamilyCount() != 1
        || !descriptor.hasColumnFamily(family)
        || descriptor.getDurability() != Durability.SYNC_WAL
        || !tableGroupId.equals(descriptor.getValue("etcdhbase.m0.table-group-id"))
        || !saltProfileId.equals(descriptor.getValue("etcdhbase.m0.salt-profile-id"))
        || !containsControlCategory(descriptor.getValue("etcdhbase.m0.record-categories"))) {
      throw new IllegalArgumentException("table does not match its M2 system binding");
    }
    var columnFamily = descriptor.getColumnFamily(family);
    if (columnFamily.getTimeToLive() != HConstants.FOREVER || columnFamily.getMaxVersions() != 1) {
      throw new IllegalArgumentException(
          "system records require a non-expiring single-version column family");
    }
  }

  private static boolean containsControlCategory(String categories) {
    if (categories == null) return false;
    return Arrays.stream(categories.split(",", -1)).anyMatch("CONTROL"::equals);
  }

  private static String requireIdentifier(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")) {
      throw new IllegalArgumentException("invalid " + name);
    }
    return value;
  }

  private static byte[] fingerprint(TableName table, byte[] family, String group,
      String saltProfileId, io.etcdhbase.spi.salt.SaltProfile profile) {
    try {
      var bytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(bytes)) {
        output.writeInt(1);
        write(output, table.getName());
        write(output, family);
        write(output, group.getBytes(StandardCharsets.UTF_8));
        write(output, saltProfileId.getBytes(StandardCharsets.UTF_8));
        write(output, profile.strategyId().getBytes(StandardCharsets.UTF_8));
        output.writeInt(profile.encodingVersion());
        output.writeInt(profile.bucketCount());
        output.writeInt(profile.saltWidthBytes());
      }
      return MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory system binding fingerprint failed", impossible);
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static void write(DataOutputStream output, byte[] value) throws IOException {
    output.writeInt(value.length);
    output.write(value);
  }
}
