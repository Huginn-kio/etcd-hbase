package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class InvocationMessagesTest {
  private static final int MAX_MANIFEST_ROOT_BYTES = 8192;
  private static final InvocationId INVOCATION = new InvocationId(
      UUID.fromString("00000000-0000-0000-0000-000000000001"));
  private static final TransactionId TRANSACTION = new TransactionId(
      UUID.fromString("00000000-0000-0000-0000-000000000002"));
  private static final ByteKey REQUEST_DIGEST = digest(1);
  private static final ByteKey AUTHORIZATION_DIGEST = digest(2);

  @Test
  void everyDurableStateRoundTripsItsImmutableBinding() {
    InvocationBinding preparing = preparing();
    InvocationBinding sealed = preparing.seal(digest(3));
    InvocationBinding selected = sealed.selected(digest(4));
    InvocationBinding windowClosed = selected.closeQueryWindow();
    InvocationBinding notSelected = InvocationBinding.restore(INVOCATION, REQUEST_DIGEST,
        AUTHORIZATION_DIGEST, TRANSACTION, 1, 0, 1, InvocationBinding.State.NOT_SELECTED,
        Optional.empty(), Optional.empty());

    for (InvocationBinding binding : List.of(
        preparing, sealed, selected, notSelected, windowClosed)) {
      assertSameBinding(binding, InvocationMessages.decode(InvocationMessages.encode(binding)));
    }
  }

  @Test
  void maximumManifestAndResultReferencesRoundTripAndNextByteIsRejected() {
    InvocationBinding maximum = InvocationBinding.restore(INVOCATION, REQUEST_DIGEST,
        AUTHORIZATION_DIGEST, TRANSACTION, 1, 0, 1, InvocationBinding.State.SELECTED,
        Optional.of(ByteKey.copyOf(new byte[MAX_MANIFEST_ROOT_BYTES])),
        Optional.of(ByteKey.copyOf(new byte[InvocationBinding.MAX_RESULT_REFERENCE_BYTES])));
    assertSameBinding(maximum, InvocationMessages.decode(InvocationMessages.encode(maximum)));

    assertThrows(IllegalArgumentException.class, () -> InvocationBinding.restore(INVOCATION,
        REQUEST_DIGEST, AUTHORIZATION_DIGEST, TRANSACTION, 1, 0, 1,
        InvocationBinding.State.SEALED,
        Optional.of(ByteKey.copyOf(new byte[MAX_MANIFEST_ROOT_BYTES + 1])),
        Optional.empty()));
    assertThrows(IllegalArgumentException.class, () -> InvocationBinding.restore(INVOCATION,
        REQUEST_DIGEST, AUTHORIZATION_DIGEST, TRANSACTION, 1, 0, 1,
        InvocationBinding.State.SELECTED, Optional.of(digest(3)),
        Optional.of(ByteKey.copyOf(new byte[InvocationBinding.MAX_RESULT_REFERENCE_BYTES + 1]))));
  }

  @Test
  void decoderRejectsMissingUnknownAndMalformedFields() throws Exception {
    CommitProtos.InvocationBinding valid = CommitProtos.InvocationBinding.parseFrom(
        InvocationMessages.encode(preparing()));
    byte[] missing = valid.toBuilder().clearTransactionId().build().toByteArray();
    assertThrows(IllegalArgumentException.class, () -> InvocationMessages.decode(missing));

    byte[] malformedId = valid.toBuilder()
        .setInvocationId(ByteString.copyFrom(new byte[15])).build().toByteArray();
    assertThrows(IllegalArgumentException.class, () -> InvocationMessages.decode(malformedId));

    byte[] malformedDigest = valid.toBuilder()
        .setRequestDigest(ByteString.copyFrom(new byte[31])).build().toByteArray();
    assertThrows(IllegalArgumentException.class, () -> InvocationMessages.decode(malformedDigest));

    byte[] unknownField = valid.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
        .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build())
        .build().toByteArray();
    assertThrows(IllegalArgumentException.class, () -> InvocationMessages.decode(unknownField));

    byte[] unspecifiedState = valid.toBuilder()
        .setState(CommitProtos.InvocationAttemptState.INVOCATION_STATE_UNSPECIFIED)
        .build().toByteArray();
    assertThrows(IllegalArgumentException.class,
        () -> InvocationMessages.decode(unspecifiedState));

    assertThrows(IllegalArgumentException.class,
        () -> InvocationMessages.decode(new byte[16 * 1024 + 1]));
  }

  private static InvocationBinding preparing() {
    return InvocationBinding.begin(INVOCATION, REQUEST_DIGEST, AUTHORIZATION_DIGEST,
        TRANSACTION, 1, 0, 1);
  }

  private static ByteKey digest(int marker) {
    byte[] value = new byte[32];
    value[0] = (byte) marker;
    return ByteKey.copyOf(value);
  }

  private static void assertSameBinding(InvocationBinding expected, InvocationBinding actual) {
    assertEquals(expected.invocationId(), actual.invocationId());
    assertEquals(expected.requestDigest(), actual.requestDigest());
    assertEquals(expected.authorizationScopeDigest(), actual.authorizationScopeDigest());
    assertEquals(expected.transactionId(), actual.transactionId());
    assertEquals(expected.preparedOwnerEpoch(), actual.preparedOwnerEpoch());
    assertEquals(expected.baseSequence(), actual.baseSequence());
    assertEquals(expected.baseRevision(), actual.baseRevision());
    assertEquals(expected.state(), actual.state());
    assertEquals(expected.manifestRoot(), actual.manifestRoot());
    assertEquals(expected.resultReference(), actual.resultReference());
  }
}
