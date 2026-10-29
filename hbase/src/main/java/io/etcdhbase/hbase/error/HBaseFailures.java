package io.etcdhbase.hbase.error;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.spi.identity.BackendIdentityException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.security.sasl.SaslException;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;

/** Classifies client failures without exposing backend diagnostics in the public message. */
public final class HBaseFailures {
  private HBaseFailures() {}

  public static BackendFailure classify(Throwable failure) {
    Throwable root = Objects.requireNonNull(failure, "failure");
    for (int depth = 0;
        depth < 16
            && (root instanceof CompletionException || root instanceof ExecutionException)
            && root.getCause() != null
            && root.getCause() != root;
        depth++) {
      root = root.getCause();
    }
    BackendFailure.Category fallback = BackendFailure.Category.UNKNOWN;
    // Bound inspection even for a malformed or cyclic backend exception chain.
    Throwable current = root;
    for (int depth = 0; current != null && depth < 16; depth++, current = current.getCause()) {
      if (current instanceof RemoteException remote) {
        Throwable decoded =
            remote.unwrapRemoteException(
                AccessDeniedException.class,
                AccessControlException.class,
                InvalidToken.class,
                SaslException.class,
                CallTimeoutException.class);
        if (decoded != remote) {
          return new BackendFailure(classify(decoded).category(), root);
        }
      }
      if (current instanceof BackendFailure known) {
        return known;
      }
      if (current instanceof BackendIdentityException identityFailure) {
        return new BackendFailure(
            switch (identityFailure.reason()) {
              case INVALID_CREDENTIALS -> BackendFailure.Category.AUTHENTICATION;
              case INSUFFICIENT_PERMISSION -> BackendFailure.Category.PERMISSION;
              case RESOURCE_LIMIT -> BackendFailure.Category.RESOURCE_LIMIT;
              case TIMEOUT -> BackendFailure.Category.TIMEOUT;
              case CANCELLED -> BackendFailure.Category.CANCELLED;
              case BACKEND_UNAVAILABLE -> BackendFailure.Category.UNAVAILABLE;
            },
            root);
      }
      if (current instanceof AccessDeniedException || current instanceof AccessControlException) {
        return new BackendFailure(BackendFailure.Category.PERMISSION, root);
      }
      if (current instanceof SaslException
          || current instanceof InvalidToken
          || current instanceof javax.security.auth.login.LoginException) {
        return new BackendFailure(BackendFailure.Category.AUTHENTICATION, root);
      }
      if (current instanceof CancellationException || current instanceof InterruptedException) {
        return new BackendFailure(BackendFailure.Category.CANCELLED, root);
      }
      if (current instanceof TimeoutException
          || current instanceof SocketTimeoutException
          || current instanceof CallTimeoutException) {
        return new BackendFailure(BackendFailure.Category.TIMEOUT, root);
      }
      if (current instanceof IOException) {
        fallback = BackendFailure.Category.UNAVAILABLE;
      }
    }
    return new BackendFailure(fallback, root);
  }
}
