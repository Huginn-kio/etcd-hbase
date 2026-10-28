package io.etcdhbase.hbase.api;

import io.etcdhbase.spi.identity.AuthenticationScope;
import java.util.concurrent.CompletionStage;
import org.apache.hadoop.hbase.client.AsyncConnection;

/**
 * Verifies the configured table-group permissions by actual backend RPCs using the supplied
 * identity's connection. Returning success without contacting the backend is not a valid verifier.
 * Implementations return promptly, bound their work, and do not mutate business data.
 */
@FunctionalInterface
public interface HBaseAccessVerifier {
  CompletionStage<Void> verify(AsyncConnection connection, AuthenticationScope scope);
}
