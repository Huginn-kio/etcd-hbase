package io.etcdhbase.hbase.codec;

import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseRenewalVersion;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

/** Encodes an invocation-bound KeepAlive response with its durable renewal version. */
public final class LeaseKeepAliveResultMessages {
  private static final int FORMAT_VERSION = 1;
  public static final int MAX_ENCODED_BYTES = 4_096;

  public record Decoded(ByteKey response, Optional<LeaseRenewalVersion> renewalVersion) {
    public Decoded {
      Objects.requireNonNull(response, "response");
      Objects.requireNonNull(renewalVersion, "renewalVersion");
      if (response.length() == 0 || response.length() > MAX_ENCODED_BYTES) {
        throw invalid();
      }
    }
  }

  private LeaseKeepAliveResultMessages() {}

  public static ByteKey encode(ByteKey response, Optional<LeaseRenewalVersion> renewalVersion) {
    Objects.requireNonNull(response, "response");
    Objects.requireNonNull(renewalVersion, "renewalVersion");
    if (response.length() == 0 || response.length() > MAX_ENCODED_BYTES) throw invalid();

    var message = CommitProtos.LeaseKeepAliveResult.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setResponse(com.google.protobuf.ByteString.copyFrom(response.bytes()));
    renewalVersion.ifPresent(version -> message
        .setLeaseId(version.identity().leaseId())
        .setIncarnation(version.identity().incarnation())
        .setRenewalGeneration(version.renewalGeneration()));
    byte[] encoded = message.build().toByteArray();
    if (encoded.length > MAX_ENCODED_BYTES) throw invalid();
    return ByteKey.copyOf(encoded);
  }

  public static Decoded decode(ByteBuffer encoded) {
    Objects.requireNonNull(encoded, "encoded");
    int encodedBytes = encoded.remaining();
    if (encodedBytes == 0 || encodedBytes > MAX_ENCODED_BYTES) throw invalid();
    byte[] bytes = new byte[encodedBytes];
    encoded.duplicate().get(bytes);
    return decode(ByteKey.copyOf(bytes));
  }

  public static Decoded decode(ByteKey encoded) {
    Objects.requireNonNull(encoded, "encoded");
    if (encoded.length() == 0 || encoded.length() > MAX_ENCODED_BYTES) throw invalid();
    try {
      var message = CommitProtos.LeaseKeepAliveResult.parseFrom(encoded.bytes());
      boolean hasAnyVersionField = message.hasLeaseId()
          || message.hasIncarnation() || message.hasRenewalGeneration();
      boolean hasAllVersionFields = message.hasLeaseId()
          && message.hasIncarnation() && message.hasRenewalGeneration();
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasResponse() || message.getResponse().isEmpty()
          || message.getResponse().size() > MAX_ENCODED_BYTES
          || hasAnyVersionField != hasAllVersionFields) {
        throw invalid();
      }
      Optional<LeaseRenewalVersion> version = hasAllVersionFields
          ? Optional.of(new LeaseRenewalVersion(
              new LeaseIdentity(message.getLeaseId(), message.getIncarnation()),
              message.getRenewalGeneration()))
          : Optional.empty();
      return new Decoded(ByteKey.copyOf(message.getResponse().toByteArray()), version);
    } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
      throw new IllegalArgumentException("invalid persisted lease KeepAlive result", failure);
    }
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("lease KeepAlive result violates the storage contract");
  }
}
