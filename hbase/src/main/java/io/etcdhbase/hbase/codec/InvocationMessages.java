package io.etcdhbase.hbase.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.util.Optional;

/** Strict wire codec for the durable, secret-free invocation binding. */
public final class InvocationMessages {
  private static final int FORMAT_VERSION = 1;
  private static final int MAX_MESSAGE_BYTES = 16 * 1024;

  private InvocationMessages() {}

  public static byte[] encode(InvocationBinding binding) {
    var builder = CommitProtos.InvocationBinding.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setInvocationId(ByteString.copyFrom(binding.invocationId().bytes()))
        .setRequestDigest(ByteString.copyFrom(binding.requestDigest().bytes()))
        .setAuthorizationScopeDigest(ByteString.copyFrom(binding.authorizationScopeDigest().bytes()))
        .setTransactionId(ByteString.copyFrom(binding.transactionId().bytes()))
        .setPreparedOwnerEpoch(binding.preparedOwnerEpoch())
        .setBaseSequence(binding.baseSequence())
        .setBaseRevision(binding.baseRevision())
        .setState(encodeState(binding.state()));
    binding.manifestRoot().ifPresent(root -> builder.setManifestRoot(ByteString.copyFrom(root.bytes())));
    binding.resultReference().ifPresent(result -> builder.setResultReference(ByteString.copyFrom(result.bytes())));
    byte[] encoded = builder.build().toByteArray();
    requireLength(encoded.length);
    return encoded;
  }

  public static InvocationBinding decode(byte[] encoded) {
    requireLength(encoded.length);
    try {
      var message = CommitProtos.InvocationBinding.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasInvocationId() || !message.hasRequestDigest()
          || !message.hasAuthorizationScopeDigest() || !message.hasTransactionId()
          || !message.hasPreparedOwnerEpoch() || !message.hasBaseSequence()
          || !message.hasBaseRevision() || !message.hasState()) {
        throw invalid();
      }
      byte[] invocation = message.getInvocationId().toByteArray();
      byte[] transaction = message.getTransactionId().toByteArray();
      if (invocation.length != 16 || transaction.length != 16) throw invalid();
      return InvocationBinding.restore(InvocationId.fromBytes(invocation),
          ByteKey.copyOf(message.getRequestDigest().toByteArray()),
          ByteKey.copyOf(message.getAuthorizationScopeDigest().toByteArray()),
          TransactionId.fromBytes(transaction), message.getPreparedOwnerEpoch(),
          message.getBaseSequence(), message.getBaseRevision(), decodeState(message.getState()),
          message.hasManifestRoot()
              ? Optional.of(ByteKey.copyOf(message.getManifestRoot().toByteArray())) : Optional.empty(),
          message.hasResultReference()
              ? Optional.of(ByteKey.copyOf(message.getResultReference().toByteArray())) : Optional.empty());
    } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
      throw new IllegalArgumentException("invalid persisted invocation binding", failure);
    }
  }

  private static CommitProtos.InvocationAttemptState encodeState(InvocationBinding.State state) {
    return switch (state) {
      case PREPARING -> CommitProtos.InvocationAttemptState.INVOCATION_PREPARING;
      case SEALED -> CommitProtos.InvocationAttemptState.INVOCATION_SEALED;
      case SELECTED -> CommitProtos.InvocationAttemptState.INVOCATION_SELECTED;
      case NOT_SELECTED -> CommitProtos.InvocationAttemptState.INVOCATION_NOT_SELECTED;
      case WINDOW_CLOSED -> CommitProtos.InvocationAttemptState.INVOCATION_WINDOW_CLOSED;
      case NO_CHANGES -> CommitProtos.InvocationAttemptState.INVOCATION_NO_CHANGES;
    };
  }

  private static InvocationBinding.State decodeState(CommitProtos.InvocationAttemptState state) {
    return switch (state) {
      case INVOCATION_PREPARING -> InvocationBinding.State.PREPARING;
      case INVOCATION_SEALED -> InvocationBinding.State.SEALED;
      case INVOCATION_SELECTED -> InvocationBinding.State.SELECTED;
      case INVOCATION_NOT_SELECTED -> InvocationBinding.State.NOT_SELECTED;
      case INVOCATION_WINDOW_CLOSED -> InvocationBinding.State.WINDOW_CLOSED;
      case INVOCATION_NO_CHANGES -> InvocationBinding.State.NO_CHANGES;
      case INVOCATION_STATE_UNSPECIFIED, UNRECOGNIZED -> throw invalid();
    };
  }

  private static void requireLength(int length) {
    if (length <= 0 || length > MAX_MESSAGE_BYTES) throw invalid();
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("invocation message violates its storage contract");
  }
}
