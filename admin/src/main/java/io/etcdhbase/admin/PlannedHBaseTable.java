package io.etcdhbase.admin;

import io.etcdhbase.admin.config.InitializationConfiguration.ColumnFamilySchema;
import io.etcdhbase.admin.config.InitializationConfiguration.TableSchemaCandidate;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/** Maps a prototype table candidate to its owned descriptor and initial region boundaries. */
record PlannedHBaseTable(TableDescriptor descriptor, byte[][] splitKeys) {
  PlannedHBaseTable {
    splitKeys = copyKeys(splitKeys);
  }

  @Override
  public byte[][] splitKeys() {
    return copyKeys(splitKeys);
  }

  static PlannedHBaseTable from(
      TableGroupInitializationPlan plan, TableSchemaCandidate table, String clusterId) {
    byte[][] splits =
        table.splitKeysHex().stream().map(HexFormat.of()::parseHex).toArray(byte[][]::new);
    var descriptor =
        TableDescriptorBuilder.newBuilder(TableName.valueOf(plan.namespace(), table.tableName()))
            .setDurability(Durability.SYNC_WAL)
            .setRegionReplication(1)
            .setValue("etcdhbase.m0.table-group-id", plan.tableGroupId())
            .setValue("etcdhbase.m0.backend-cluster-id", clusterId)
            .setValue("etcdhbase.m0.schema-version", Integer.toString(plan.schemaVersion()))
            .setValue("etcdhbase.m0.table-schema-version", Integer.toString(table.schemaVersion()))
            .setValue("etcdhbase.m0.profile-version", Integer.toString(table.profileVersion()))
            .setValue("etcdhbase.m0.salt-profile-id", plan.saltProfile().id())
            .setValue("etcdhbase.m0.history-layout", plan.historyLayout().name())
            .setValue("etcdhbase.m0.plan-fingerprint", plan.fingerprint())
            .setValue(
                "etcdhbase.m0.record-categories",
                table.recordCategories().stream()
                    .map(Enum::name)
                    .sorted()
                    .collect(java.util.stream.Collectors.joining(",")))
            .setValue("etcdhbase.m0.split-keys-sha256", splitFingerprint(splits));
    table.columnFamilies().stream()
        .map(PlannedHBaseTable::columnFamily)
        .forEach(descriptor::setColumnFamily);
    return new PlannedHBaseTable(descriptor.build(), splits);
  }

  boolean matches(TableDescriptor actual) {
    if (!descriptor.getTableName().equals(actual.getTableName())
        || descriptor.getColumnFamilyCount() != actual.getColumnFamilyCount()) {
      return false;
    }
    // HBase can add server-owned defaults. Every explicitly planned value must still agree.
    for (Map.Entry<Bytes, Bytes> attribute : descriptor.getValues().entrySet()) {
      if (!attribute.getValue().equals(actual.getValue(attribute.getKey()))) {
        return false;
      }
    }
    for (ColumnFamilyDescriptor expected : descriptor.getColumnFamilies()) {
      ColumnFamilyDescriptor observed = actual.getColumnFamily(expected.getName());
      if (observed == null
          || expected.getMinVersions() != observed.getMinVersions()
          || expected.getMaxVersions() != observed.getMaxVersions()
          || expected.getTimeToLive() != observed.getTimeToLive()
          || expected.getKeepDeletedCells() != observed.getKeepDeletedCells()) {
        return false;
      }
    }
    return true;
  }

  boolean matchesInitialRegions(List<RegionInfo> regions) {
    List<byte[]> expectedStarts = new ArrayList<>();
    expectedStarts.add(new byte[0]);
    expectedStarts.addAll(Arrays.asList(splitKeys));
    var actual =
        regions.stream()
            .filter(region -> region.getReplicaId() == 0 && !region.isSplitParent())
            .sorted(Comparator.comparing(RegionInfo::getStartKey, Bytes.BYTES_COMPARATOR))
            .toList();
    if (expectedStarts.size() != actual.size()) {
      return false;
    }
    for (int index = 0; index < expectedStarts.size(); index++) {
      byte[] expectedEnd =
          index + 1 == expectedStarts.size() ? new byte[0] : expectedStarts.get(index + 1);
      if (!Arrays.equals(expectedStarts.get(index), actual.get(index).getStartKey())
          || !Arrays.equals(expectedEnd, actual.get(index).getEndKey())) {
        return false;
      }
    }
    return true;
  }

  private static ColumnFamilyDescriptor columnFamily(ColumnFamilySchema family) {
    return ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family.name()))
        .setMinVersions(family.minVersions())
        .setMaxVersions(family.maxVersions())
        .setTimeToLive(family.ttlSeconds() == -1 ? HConstants.FOREVER : family.ttlSeconds())
        .setKeepDeletedCells(KeepDeletedCells.valueOf(family.keepDeletedCells().name()))
        .build();
  }

  private static String splitFingerprint(byte[][] splits) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (byte[] split : splits) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(split.length).array());
        digest.update(split);
      }
      return HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static byte[][] copyKeys(byte[][] values) {
    return Arrays.stream(values).map(byte[]::clone).toArray(byte[][]::new);
  }
}
