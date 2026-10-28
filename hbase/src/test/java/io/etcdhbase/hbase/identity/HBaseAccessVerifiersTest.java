package io.etcdhbase.hbase.identity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.etcdhbase.hbase.identity.HBaseAccessVerifiers.TableAccess;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendIdentityException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.junit.jupiter.api.Test;

class HBaseAccessVerifiersTest {
  @Test
  void verifiesAllConfiguredTablesAndActionsAndRejectsPartialOrAbsentPermissions()
      throws Exception {
    var connection = mock(AsyncConnection.class);
    var admin = mock(AsyncAdmin.class);
    when(connection.getAdmin()).thenReturn(admin);
    when(admin.hasUserPermissions(anyList()))
        .thenAnswer(
            invocation -> {
              List<org.apache.hadoop.hbase.security.access.Permission> permissions =
                  invocation.getArgument(0);
              assertEquals(2, permissions.size());
              for (var permission : permissions) {
                assertTrue(permission.implies(Action.READ));
                assertTrue(permission.implies(Action.WRITE));
              }
              return CompletableFuture.completedFuture(List.of(true, true));
            });
    var verifier =
        HBaseAccessVerifiers.tablePermissions(
            List.of(
                new TableAccess(TableName.valueOf("ns:a"), Set.of(Action.READ, Action.WRITE)),
                new TableAccess(TableName.valueOf("ns:b"), Set.of(Action.READ, Action.WRITE))));
    var scope = new AuthenticationScope("group", "backend");
    verifier.verify(connection, scope).toCompletableFuture().get();
    for (List<Boolean> permissions :
        Arrays.asList(List.of(true, false), List.of(true), Arrays.asList(true, null))) {
      doReturn(CompletableFuture.completedFuture(permissions))
          .when(admin)
          .hasUserPermissions(anyList());
      var failure =
          assertThrows(
              ExecutionException.class,
              () -> verifier.verify(connection, scope).toCompletableFuture().get());
      assertEquals(
          BackendIdentityException.Reason.INSUFFICIENT_PERMISSION,
          assertInstanceOf(BackendIdentityException.class, failure.getCause()).reason());
    }
  }
}
