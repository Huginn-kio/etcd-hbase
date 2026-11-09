package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** Immutable Put input after protocol-independent static validation. */
public final class PutCommand {
  public enum LeaseIntent { NONE, EXPLICIT, PRESERVE }

  private final ByteKey key;
  private final byte[] value;
  private final long leaseId;
  private final boolean previousRequested;
  private final boolean ignoreValue;
  private final LeaseIntent leaseIntent;

  public PutCommand(ByteKey key, byte[] value, long leaseId, boolean previousRequested,
      boolean ignoreValue, boolean ignoreLease) {
    this.key = Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    // Match the fixed etcd validation order when more than one field is invalid.
    if (key.length() == 0) throw new KvFailure(KvFailure.Reason.EMPTY_KEY);
    if (ignoreValue && value.length != 0) throw new KvFailure(KvFailure.Reason.VALUE_PROVIDED);
    if (ignoreLease && leaseId != 0) throw new KvFailure(KvFailure.Reason.LEASE_PROVIDED);
    this.value = value.clone();
    this.leaseId = leaseId;
    this.previousRequested = previousRequested;
    this.ignoreValue = ignoreValue;
    if (ignoreLease) {
      this.leaseIntent = LeaseIntent.PRESERVE;
    } else if (leaseId == 0) {
      this.leaseIntent = LeaseIntent.NONE;
    } else {
      this.leaseIntent = LeaseIntent.EXPLICIT;
    }
  }

  public ByteKey key() { return key; }
  public byte[] value() { return value.clone(); }
  public int valueLength() { return value.length; }
  public long leaseId() { return leaseId; }
  public boolean previousRequested() { return previousRequested; }
  public boolean ignoreValue() { return ignoreValue; }
  public LeaseIntent leaseIntent() { return leaseIntent; }
}
