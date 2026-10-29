package io.etcdhbase.admin;

import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;

/** The initializer's synchronous operation boundary; it never owns the borrowed connection. */
interface TableGroupAdmin {
  String clusterId() throws Exception;

  List<String> listNamespaces() throws Exception;

  void createNamespace(String namespace) throws Exception;

  List<TableName> listTableNames(String namespace) throws Exception;

  TableDescriptor getDescriptor(TableName table) throws Exception;

  List<RegionInfo> getRegions(TableName table) throws Exception;

  /** Includes retained versions and tombstones; logical invisibility alone is insufficient. */
  boolean isEmpty(TableName table) throws Exception;

  void createTable(TableDescriptor descriptor, byte[][] splitKeys) throws Exception;
}
