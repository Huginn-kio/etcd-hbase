package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseRenewalVersion;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class LeaseKeepAliveResultMessagesTest {
  @Test
  void roundTripsResponseAndRenewedLeaseIdentityAndGeneration() {
    var response = ByteKey.copyOf(new byte[] {3, 1, 4});
    var version = new LeaseRenewalVersion(new LeaseIdentity(-7, 3), 9);

    var decoded = LeaseKeepAliveResultMessages.decode(
        LeaseKeepAliveResultMessages.encode(response, Optional.of(version)));

    assertEquals(response, decoded.response());
    assertEquals(Optional.of(version), decoded.renewalVersion());
  }

  @Test
  void roundTripsNoChangeResultWithoutRenewalVersion() {
    var response = ByteKey.copyOf(new byte[] {2, 7});

    var decoded = LeaseKeepAliveResultMessages.decode(
        LeaseKeepAliveResultMessages.encode(response, Optional.empty()));

    assertEquals(response, decoded.response());
    assertEquals(Optional.empty(), decoded.renewalVersion());
  }

  @Test
  void rejectsMissingFieldsUnknownFormatAndPartialRenewalVersion() throws Exception {
    var encoded = LeaseKeepAliveResultMessages.encode(ByteKey.copyOf(new byte[] {5}),
        Optional.of(new LeaseRenewalVersion(new LeaseIdentity(11, 2), 4)));
    var message = CommitProtos.LeaseKeepAliveResult.parseFrom(encoded.bytes());

    assertInvalid(message.toBuilder().clearResponse().build());
    assertInvalid(message.toBuilder().setFormatVersion(2).build());
    assertInvalid(message.toBuilder().clearIncarnation().build());
    assertInvalid(message.toBuilder().setIncarnation(0).build());
    assertInvalid(message.toBuilder().setRenewalGeneration(0).build());
    assertThrows(IllegalArgumentException.class,
        () -> LeaseKeepAliveResultMessages.decode(ByteKey.copyOf(new byte[] {(byte) 0xff})));
  }

  @Test
  void decodesBoundedBuffersAndRejectsOversizedPayloads() {
    var encoded = LeaseKeepAliveResultMessages.encode(ByteKey.copyOf(new byte[] {9}),
        Optional.of(new LeaseRenewalVersion(new LeaseIdentity(11, 2), 4)));

    var decoded = LeaseKeepAliveResultMessages.decode(ByteBuffer.wrap(encoded.bytes()));
    assertEquals(ByteKey.copyOf(new byte[] {9}), decoded.response());
    assertEquals(Optional.of(new LeaseRenewalVersion(new LeaseIdentity(11, 2), 4)),
        decoded.renewalVersion());
    assertThrows(IllegalArgumentException.class, () -> LeaseKeepAliveResultMessages.decode(
        ByteBuffer.allocate(LeaseKeepAliveResultMessages.MAX_ENCODED_BYTES + 1)));
  }

  private static void assertInvalid(CommitProtos.LeaseKeepAliveResult message) {
    assertThrows(IllegalArgumentException.class,
        () -> LeaseKeepAliveResultMessages.decode(ByteKey.copyOf(message.toByteArray())));
  }
}
