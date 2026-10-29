package io.etcdhbase.hbase.api;

import java.io.IOException;
import org.apache.hadoop.hbase.security.User;

/**
 * Owns one isolated backend user and its credentials; close its connections before this context.
 */
public interface HBaseUserContext extends AutoCloseable {
  /** A borrowed user for the HBase adapter only. Never expose it through the common SPI. */
  User user();

  @Override
  void close() throws IOException;
}
