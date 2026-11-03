package io.etcdhbase.hbase.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.hbase.storage.proto.CommitProtos;

/** Strict codec for immutable per-sequence commit decisions. */
public final class CommitDecisionMessages {
  public static final int FORMAT_VERSION = 1;
  public static final int MAX_BYTES = 16 * 1024;

  private CommitDecisionMessages() {}

  public static byte[] encode(CommitDecision decision) {
    var message = CommitProtos.CommitDecision.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setTransactionId(ByteString.copyFrom(decision.transactionId().bytes()))
        .setInvocationId(ByteString.copyFrom(decision.invocationId().bytes()))
        .setSequence(decision.sequence())
        .setBaseRevision(decision.baseRevision())
        .setRevision(decision.revision())
        .setManifestRoot(ByteString.copyFrom(decision.manifestRoot().bytes()))
        .setManifestDigest(ByteString.copyFrom(decision.manifestDigest().bytes()))
        .setContainsKeyChanges(decision.containsKeyChanges())
        .build();
    byte[] encoded = message.toByteArray();
    requireLength(encoded.length);
    return encoded;
  }

  public static CommitDecision decode(byte[] encoded) {
    requireLength(encoded.length);
    try {
      var message = CommitProtos.CommitDecision.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasTransactionId() || !message.hasInvocationId()
          || !message.hasSequence() || !message.hasBaseRevision() || !message.hasRevision()
          || !message.hasManifestRoot()
          || !message.hasManifestDigest() || !message.hasContainsKeyChanges()) {
        throw invalid();
      }
      byte[] txn = message.getTransactionId().toByteArray();
      byte[] invocation = message.getInvocationId().toByteArray();
      byte[] root = message.getManifestRoot().toByteArray();
      byte[] digest = message.getManifestDigest().toByteArray();
      if (txn.length != 16 || invocation.length != 16 || root.length == 0 || root.length > 8192
          || digest.length != 32) throw invalid();
      return new CommitDecision(
          TransactionId.fromBytes(txn), InvocationId.fromBytes(invocation),
          message.getSequence(), message.getBaseRevision(), message.getRevision(), ByteKey.copyOf(root),
          ByteKey.copyOf(digest), message.getContainsKeyChanges());
    } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
      throw new IllegalArgumentException("invalid persisted commit decision", failure);
    }
  }

  private static void requireLength(int length) {
    if (length <= 0 || length > MAX_BYTES) throw invalid();
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("commit decision violates the storage contract");
  }
}
