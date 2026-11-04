package io.etcdhbase.hbase.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.util.Objects;

/** Strict M2 row-local fence/guard state; it is not a business metadata cell. */
public final class RowFenceMessages {
  public static final int FORMAT_VERSION = 1;
  public static final int MAX_BYTES = 4096;

  public enum RowKind { CURRENT(1), HISTORY(2), NATIVE(3);
    private final int id;
    RowKind(int id) { this.id = id; }
    public int id() { return id; }
    private static RowKind from(int id) {
      for (RowKind kind : values()) if (kind.id == id) return kind;
      throw new IllegalArgumentException("unknown row fence kind");
    }
  }

  public enum State { PRESENT(1), RETIRED(2);
    private final int id;
    State(int id) { this.id = id; }
    public int id() { return id; }
    private static State from(int id) {
      for (State state : values()) if (state.id == id) return state;
      throw new IllegalArgumentException("unknown row fence state");
    }
  }

  public record Fence(RowKind rowKind, State state, long appliedSequence, TransactionId transactionId,
      long revision, ByteKey contentDigest, long maintenanceGeneration, long ownerEpoch,
      long baseSequence) {
    public Fence {
      Objects.requireNonNull(rowKind, "rowKind");
      Objects.requireNonNull(state, "state");
      Objects.requireNonNull(transactionId, "transactionId");
      Objects.requireNonNull(contentDigest, "contentDigest");
      if (appliedSequence <= 0 || revision <= 0 || contentDigest.length() != 32
          || maintenanceGeneration < 0 || ownerEpoch <= 0 || baseSequence < 0
          || appliedSequence != Math.addExact(baseSequence, 1)) {
        throw new IllegalArgumentException("invalid row fence");
      }
    }
  }

  private RowFenceMessages() {}

  public static byte[] encode(Fence fence) {
    byte[] encoded = CommitProtos.RowFence.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setRowKind(fence.rowKind().id())
        .setState(fence.state().id())
        .setAppliedSequence(fence.appliedSequence())
        .setTransactionId(ByteString.copyFrom(fence.transactionId().bytes()))
        .setRevision(fence.revision())
        .setContentDigest(ByteString.copyFrom(fence.contentDigest().bytes()))
        .setMaintenanceGeneration(fence.maintenanceGeneration())
        .setOwnerEpoch(fence.ownerEpoch())
        .setBaseSequence(fence.baseSequence())
        .build().toByteArray();
    requireEncodedLength(encoded.length);
    return encoded;
  }

  public static Fence decode(byte[] encoded) {
    requireEncodedLength(encoded.length);
    try {
      var message = CommitProtos.RowFence.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasRowKind() || !message.hasState() || !message.hasAppliedSequence()
          || !message.hasTransactionId() || !message.hasRevision() || !message.hasContentDigest()
          || !message.hasMaintenanceGeneration() || !message.hasOwnerEpoch()
          || !message.hasBaseSequence()) throw invalid();
      byte[] txn = message.getTransactionId().toByteArray();
      byte[] digest = message.getContentDigest().toByteArray();
      if (txn.length != 16 || digest.length != 32) throw invalid();
      return new Fence(RowKind.from(message.getRowKind()), State.from(message.getState()),
          message.getAppliedSequence(), TransactionId.fromBytes(txn), message.getRevision(),
          ByteKey.copyOf(digest), message.getMaintenanceGeneration(), message.getOwnerEpoch(),
          message.getBaseSequence());
    } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
      throw new IllegalArgumentException("invalid persisted row fence", failure);
    }
  }

  /** Checks a persisted fence cell length before its backing bytes are copied. */
  public static void requireEncodedLength(int length) {
    if (length <= 0 || length > MAX_BYTES) throw invalid();
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("row fence violates the storage contract");
  }
}
