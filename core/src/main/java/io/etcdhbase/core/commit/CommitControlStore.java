package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Internal authoritative control-row port; absent state is never initialized implicitly. */
public interface CommitControlStore {
  CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope);

  /** Explicit bootstrap only; a false result requires a strong reread and identity check. */
  CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState, RequestScope scope);

  /** Compares the exact bytes returned by read before replacing the single control row. */
  CompletableFuture<Boolean> compareAndSet(
      ControlSnapshot observed, CommitState replacement, RequestScope scope);
}
