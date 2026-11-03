package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.spi.salt.HashSaltStrategy;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

class SystemTableBindingTest {
  private static final TableName TABLE = TableName.valueOf("m2", "system");
  private static final byte[] FAMILY = {'s'};
  private static final String TABLE_GROUP = "system-group";
  private static final String SALT_PROFILE = "system-salt";
  private static final HashSaltStrategy SALT = new HashSaltStrategy();
  private static final SystemTableBinding BINDING =
      new SystemTableBinding(TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, SALT);

  @Test
  void acceptsTheExactDurableSingleVersionSystemTable() {
    BINDING.validate(descriptor(TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, "CONTROL",
        Durability.SYNC_WAL, HConstants.FOREVER, 1));
  }

  @Test
  void rejectsTableFamilyProfileCategoryAndDurabilityMismatches() {
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TableName.valueOf("m2", "other"), FAMILY, TABLE_GROUP, SALT_PROFILE, "CONTROL",
        Durability.SYNC_WAL, HConstants.FOREVER, 1)));
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, new byte[] {'x'}, TABLE_GROUP, SALT_PROFILE, "CONTROL",
        Durability.SYNC_WAL, HConstants.FOREVER, 1)));
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, FAMILY, "other-group", SALT_PROFILE, "CONTROL",
        Durability.SYNC_WAL, HConstants.FOREVER, 1)));
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, FAMILY, TABLE_GROUP, "other-salt", "CONTROL",
        Durability.SYNC_WAL, HConstants.FOREVER, 1)));
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, "PREPARATION",
        Durability.SYNC_WAL, HConstants.FOREVER, 1)));
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, "CONTROL",
        Durability.ASYNC_WAL, HConstants.FOREVER, 1)));
  }

  @Test
  void rejectsExpiringOrMultiVersionFamilies() {
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, "CONTROL",
        Durability.SYNC_WAL, 60, 1)));
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, "CONTROL",
        Durability.SYNC_WAL, HConstants.FOREVER, 2)));
  }

  private static TableDescriptor descriptor(TableName table, byte[] family, String tableGroup,
      String saltProfile, String categories, Durability durability, int ttl, int maxVersions) {
    var column = ColumnFamilyDescriptorBuilder.newBuilder(family)
        .setTimeToLive(ttl).setMaxVersions(maxVersions).build();
    return TableDescriptorBuilder.newBuilder(table).setColumnFamily(column)
        .setDurability(durability).setValue("etcdhbase.m0.table-group-id", tableGroup)
        .setValue("etcdhbase.m0.salt-profile-id", saltProfile)
        .setValue("etcdhbase.m0.record-categories", categories).build();
  }
}
