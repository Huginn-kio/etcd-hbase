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
import org.apache.hadoop.hbase.security.AccessDeniedException;
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
      if (current instanceof BackendFailure known) {
        return known;
      }
      if (current instanceof BackendIdentityException identityFailure) {
        return new BackendFailure(
            switch (identityFailure.reason()) {
              case INVALID_CREDENTIALS -> BackendFailure.Category.AUTHENTICATION;
              case INSUFFICIENT_PERMISSION -> BackendFailure.Category.PERMISSION;
              case BACKEND_UNAVAILABLE -> BackendFailure.Category.UNAVAILABLE;
            },
            root);
      }
      if (current instanceof AccessDeniedException || current instanceof AccessControlException) {
        return new BackendFailure(BackendFailure.Category.PERMISSION, root);
      }
      if (current instanceof SaslException || current instanceof InvalidToken) {
        return new BackendFailure(BackendFailure.Category.AUTHENTICATION, root);
      }
      if (current instanceof CancellationException || current instanceof InterruptedException) {
        return new BackendFailure(BackendFailure.Category.CANCELLED, root);
      }
      if (current instanceof TimeoutException || current instanceof SocketTimeoutException) {
        return new BackendFailure(BackendFailure.Category.TIMEOUT, root);
      }
      if (current instanceof IOException) {
        fallback = BackendFailure.Category.UNAVAILABLE;
      }
    }
    return new BackendFailure(fallback, root);
  }
}
