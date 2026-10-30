package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.junit.jupiter.api.Test;

class HBaseRecordStoreTest {
  @Test
  void cancellationDoesNotReleaseAnOperationBeforeItsBackendCallback() {
    var connection = mock(AsyncConnection.class);
    var admin = mock(AsyncAdmin.class);
    var factory = mock(HBaseConnectionFactory.class);
    var identity = mock(AuthorizedBackendIdentity.class);
    AsyncTableBuilder<?> builder = mock(AsyncTableBuilder.class, RETURNS_SELF);
    AsyncTable<?> table = mock(AsyncTable.class);
    var binding = new StorageTableBinding(TableName.valueOf("synthetic"), new byte[] {'d'},
        StorageTableBinding.Layout.CURRENT, new HashSaltStrategy());
    doReturn(CompletableFuture.completedFuture(connection)).when(factory).connectionFor(identity);
    doReturn(admin).when(connection).getAdmin();
    doReturn(CompletableFuture.completedFuture(binding.descriptor())).when(admin).getDescriptor(binding.table());
    doReturn(builder).when(connection).getTableBuilder(binding.table());
    doReturn(table).when(builder).build();
    var backend = new CompletableFuture<Result>();
    doReturn(backend).when(table).get(any(Get.class));
    try (var scope = RequestScope.withoutDeadline()) {
      var store = HBaseRecordStore.open(factory, identity, binding, scope, Runnable::run).join();
      var result = store.get(ByteKey.empty(), Long.MAX_VALUE, ReadProjection.METADATA);
      scope.cancel();
      assertFalse(backend.isCancelled(), "shared/client authority is not request cancellation state");
      assertFalse(result.isDone(), "an in-flight backend operation still owns its physical permit");
      backend.complete(Result.EMPTY_RESULT);
      var failure = assertThrows(CompletionException.class, result::join);
      assertEquals(BackendFailure.Category.CANCELLED, ((BackendFailure) failure.getCause()).category());
    }
  }
}
