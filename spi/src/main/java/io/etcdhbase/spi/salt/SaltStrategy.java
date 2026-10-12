package io.etcdhbase.spi.salt;

/**
 * Creates the fixed-width prefix for a complete logical record identifier.
 *
 * <p>The implementation must be deterministic for the same identifier and parameters, must not
 * modify the input, and must return a new byte array owned by the caller. A range scan must use the
 * same profile as point reads; implementations must not add a cluster-routing identifier.
 */
public interface SaltStrategy {
  String strategyId();

  byte[] prefixFor(byte[] logicalIdentifier, SaltParameters parameters);
}
