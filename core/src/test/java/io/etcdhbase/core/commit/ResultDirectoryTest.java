package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.List;
import org.junit.jupiter.api.Test;

class ResultDirectoryTest {
  @Test
  void bindsOrderedPagesToTheImmutableInvocationAttempt() {
    InvocationBinding binding = binding();
    ResultIdentity identity = ResultIdentity.from(binding);
    byte[] firstBytes = {1, 2};
    ResultPage first = new ResultPage(identity, 0, firstBytes);
    ResultPage second = new ResultPage(identity, 1, new byte[] {3});
    ResultDirectory directory = ResultDirectory.create(identity,
        List.of(ResultPageReference.from(first), ResultPageReference.from(second)));
    firstBytes[0] = 9;
    byte[] exported = first.payload();
    exported[0] = 8;

    assertArrayEquals(new byte[] {1, 2}, first.payload());
    assertEquals(3, directory.totalBytes());
    assertEquals(2, directory.reference().pageCount());
    assertTrue(directory.identity().matches(binding.seal(ByteKey.copyOf(new byte[] {1}))));
    assertFalse(directory.identity().matches(binding()));
    assertEquals(directory.digest(), ResultDirectory.restore(identity,
        directory.pages(), directory.digest()).digest());
    assertEquals(directory.reference(), ResultReference.fromToken(identity, directory.reference().token()));
    assertFalse(directory.reference().equals(ResultReference.fromToken(
        ResultIdentity.from(binding()), directory.reference().token())));
  }

  @Test
  void rejectsMissingReorderedAndOversizedPagesAndTamperedDigests() {
    ResultIdentity identity = ResultIdentity.from(binding());
    ResultPage first = new ResultPage(identity, 0, new byte[] {1});
    ResultPage second = new ResultPage(identity, 1, new byte[] {2});
    ResultPageReference firstReference = ResultPageReference.from(first);
    ResultPageReference secondReference = ResultPageReference.from(second);
    assertThrows(IllegalArgumentException.class,
        () -> ResultDirectory.create(identity, List.of(secondReference)));
    assertThrows(IllegalArgumentException.class,
        () -> ResultDirectory.create(identity, List.of(secondReference, firstReference)));
    assertThrows(IllegalArgumentException.class,
        () -> new ResultPage(identity, 0, new byte[ResultPage.MAX_PAYLOAD_BYTES + 1]));
    assertThrows(IllegalArgumentException.class,
        () -> new ResultPage(identity, -1, new byte[] {1}));
    assertThrows(IllegalArgumentException.class,
        () -> ResultPage.restore(identity, 0, new byte[] {2}, first.digest()));
    ResultDirectory directory = ResultDirectory.create(identity, List.of(firstReference));
    assertThrows(IllegalArgumentException.class,
        () -> ResultDirectory.restore(identity, List.of(secondReference), directory.digest()));
    ResultPage foreign = new ResultPage(ResultIdentity.from(binding()), 0, new byte[] {1});
    assertThrows(IllegalArgumentException.class,
        () -> ResultDirectory.create(identity, List.of(ResultPageReference.from(foreign))));
    assertFalse(firstReference.matches(foreign));
    assertFalse(directory.reference().matches(ResultDirectory.create(identity,
        List.of(ResultPageReference.from(new ResultPage(identity, 0, new byte[] {9}))))));
    assertThrows(IllegalArgumentException.class,
        () -> ResultReference.fromToken(identity, ByteKey.copyOf(new byte[47])));
    byte[] version = directory.reference().token().bytes();
    version[3] = 2;
    assertThrows(IllegalArgumentException.class,
        () -> ResultReference.fromToken(identity, ByteKey.copyOf(version)));
  }

  private static InvocationBinding binding() {
    return InvocationBinding.begin(InvocationId.create(), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1);
  }
}
