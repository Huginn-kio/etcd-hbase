package io.etcdhbase.hbase.api;

import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.concurrent.CompletionStage;
import org.apache.hadoop.hbase.client.AsyncConnection;

/** Manages shared HBase connections for authorized backend identities. */
public interface HBaseConnectionFactory extends AutoCloseable {
  /**
   * Borrows a manager-owned connection. Callers must not close it; only this factory closes shared
   * connections during factory shutdown. The factory must validate the handle against its issuing
   * provider state; a caller-constructed DTO alone is not authorization evidence.
   *
   * <p>The stage may be manager-owned. Request cancellation must affect a dependent copy, not the
   * shared source stage or connection. Connections must close before the credential contexts they
   * borrow.
   */
  CompletionStage<AsyncConnection> connectionFor(AuthorizedBackendIdentity identity);

  /** Idempotently stops new borrowing and closes factory-owned resources. */
  @Override
  void close();
}
