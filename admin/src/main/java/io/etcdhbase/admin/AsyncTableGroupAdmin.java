package io.etcdhbase.admin;

import static io.etcdhbase.admin.HBaseOperationAwaiter.OPERATION_TIMEOUT;
import static io.etcdhbase.admin.HBaseOperationAwaiter.await;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

/** Official HBase adapter. Its caller retains ownership of the authorized connection. */
final class AsyncTableGroupAdmin implements TableGroupAdmin {
  private final AsyncConnection connection;
  private final AsyncAdmin admin;

  AsyncTableGroupAdmin(AsyncConnection connection) {
    this.connection = connection;
    this.admin = connection.getAdmin();
  }

  @Override
  public String clusterId() throws Exception {
    return await(admin.getClusterMetrics(), OPERATION_TIMEOUT).getClusterId();
  }

  @Override
  public List<String> listNamespaces() throws Exception {
    return await(admin.listNamespaces(), OPERATION_TIMEOUT);
  }

  @Override
  public void createNamespace(String namespace) throws Exception {
    await(admin.createNamespace(NamespaceDescriptor.create(namespace).build()), OPERATION_TIMEOUT);
  }

  @Override
  public List<TableName> listTableNames(String namespace) throws Exception {
    return await(admin.listTableNamesByNamespace(namespace), OPERATION_TIMEOUT);
  }

  @Override
  public TableDescriptor getDescriptor(TableName table) throws Exception {
    return await(admin.getDescriptor(table), OPERATION_TIMEOUT);
  }

  @Override
  public List<RegionInfo> getRegions(TableName table) throws Exception {
    return await(admin.getRegions(table), OPERATION_TIMEOUT);
  }

  @Override
  public boolean isEmpty(TableName table) throws Exception {
    var scan =
        new Scan()
            .setRaw(true)
            .readAllVersions()
            .setLimit(1)
            .setCaching(1)
            .setMaxResultSize(1024)
            // A logically deleted row can still contain versions or tombstones. Only a raw,
            // key-only read can establish that a partial prototype has no retained cells.
            .setFilter(new FilterList(new FirstKeyOnlyFilter(), new KeyOnlyFilter()));
    var bounded =
        connection
            .getTableBuilder(table)
            .setOperationTimeout(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
            .setScanTimeout(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
            .build();
    return await(bounded.scanAll(scan), OPERATION_TIMEOUT).isEmpty();
  }

  @Override
  public void createTable(TableDescriptor descriptor, byte[][] splitKeys) throws Exception {
    await(admin.createTable(descriptor, splitKeys), OPERATION_TIMEOUT);
  }
}
