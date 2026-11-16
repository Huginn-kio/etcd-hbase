package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.UnknownFieldSet;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.EventReference;
import io.etcdhbase.core.commit.EventRevisionIndex;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import org.junit.jupiter.api.Test;

class EventRevisionIndexMessagesTest {
  @Test
  void roundTripsSelectedDecisionAndEventRoot() {
    EventRevisionIndex original = index();
    assertEquals(original, EventRevisionIndexMessages.decode(EventRevisionIndexMessages.encode(original)));
  }

  @Test
  void rejectsMissingFieldsUnknownFieldsAndMalformedRecords() throws Exception {
    byte[] encoded = EventRevisionIndexMessages.encode(index());
    var message = CommitProtos.EventRevisionIndex.parseFrom(encoded);
    var unknown = UnknownFieldSet.newBuilder().addField(100,
        UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    assertThrows(IllegalArgumentException.class, () -> EventRevisionIndexMessages.decode(new byte[0]));
    assertThrows(IllegalArgumentException.class,
        () -> EventRevisionIndexMessages.decode(message.toBuilder().clearEventRoot().build().toByteArray()));
    assertThrows(IllegalArgumentException.class,
        () -> EventRevisionIndexMessages.decode(message.toBuilder().setUnknownFields(unknown).build().toByteArray()));
    assertThrows(IllegalArgumentException.class,
        () -> EventRevisionIndexMessages.decode(message.toBuilder().setFormatVersion(2).build().toByteArray()));
  }

  private static EventRevisionIndex index() {
    ByteKey identity = ByteKey.copyOf(new byte[] {1});
    ByteKey digest = ByteKey.copyOf(new byte[32]);
    var decision = new CommitDecision(TransactionId.create(), InvocationId.create(),
        4, 8, 9, identity, digest, true);
    var events = new EventReference(identity, digest, 0, 0, 1, 8, 1, 0);
    return new EventRevisionIndex(decision, events);
  }
}
