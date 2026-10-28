package io.etcdhbase.hbase.error;

import static org.junit.jupiter.api.Assertions.*;

import io.etcdhbase.core.error.BackendFailure;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletionException;
import javax.security.sasl.SaslException;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.junit.jupiter.api.Test;

class HBaseFailuresTest {
  @Test
  void distinguishesRealHBasePermissionAndSaslFailuresFromTransportFailures() {
    assertEquals(
        BackendFailure.Category.PERMISSION,
        HBaseFailures.classify(
                new RemoteException(AccessDeniedException.class.getName(), "private"))
            .category());
    assertEquals(
        BackendFailure.Category.AUTHENTICATION,
        HBaseFailures.classify(new RemoteException(InvalidToken.class.getName(), "private"))
            .category());
    assertEquals(
        BackendFailure.Category.TIMEOUT,
        HBaseFailures.classify(new CallTimeoutException("private")).category());
    assertEquals(
        BackendFailure.Category.PERMISSION,
        HBaseFailures.classify(new IOException(new AccessDeniedException("private"))).category());
    assertEquals(
        BackendFailure.Category.AUTHENTICATION,
        HBaseFailures.classify(new CompletionException(new SaslException("private"))).category());
    assertEquals(
        BackendFailure.Category.TIMEOUT,
        HBaseFailures.classify(new SocketTimeoutException("private")).category());
    assertEquals(
        BackendFailure.Category.UNAVAILABLE,
        HBaseFailures.classify(new IOException("private")).category());
    assertEquals(
        BackendFailure.Category.UNKNOWN,
        HBaseFailures.classify(new IllegalStateException("private")).category());
    assertFalse(
        HBaseFailures.classify(new SaslException("private")).getMessage().contains("private"));
  }
}
