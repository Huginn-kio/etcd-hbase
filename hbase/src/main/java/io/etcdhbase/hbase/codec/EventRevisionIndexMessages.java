package io.etcdhbase.hbase.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.commit.EventRevisionIndex;
import io.etcdhbase.hbase.storage.proto.CommitProtos;

/** Versioned codec for immutable event revision lookup records. */
public final class EventRevisionIndexMessages {
  public static final int FORMAT_VERSION = 1;
  public static final int MAX_BYTES = 32 * 1024;

  private EventRevisionIndexMessages() {}

  public static byte[] encode(EventRevisionIndex index) {
    var message = CommitProtos.EventRevisionIndex.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setDecision(ByteString.copyFrom(CommitDecisionMessages.encode(index.decision())))
        .setEventRoot(ManifestMessages.encode(index.events()))
        .build();
    byte[] encoded = message.toByteArray();
    requireLength(encoded.length);
    return encoded;
  }

  public static EventRevisionIndex decode(byte[] encoded) {
    requireLength(encoded.length);
    try {
      var message = CommitProtos.EventRevisionIndex.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty() || !message.hasFormatVersion()
          || message.getFormatVersion() != FORMAT_VERSION || !message.hasDecision()
          || !message.hasEventRoot()) {
        throw invalid();
      }
      return new EventRevisionIndex(CommitDecisionMessages.decode(message.getDecision().toByteArray()),
          ManifestMessages.decodeEventReference(message.getEventRoot()));
    } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
      throw new IllegalArgumentException("invalid persisted event revision index", failure);
    }
  }

  private static void requireLength(int length) {
    if (length <= 0 || length > MAX_BYTES) throw invalid();
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("event revision index violates the storage contract");
  }
}
