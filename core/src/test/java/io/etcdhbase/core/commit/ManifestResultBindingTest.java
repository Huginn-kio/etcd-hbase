package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ManifestResultBindingTest {
  @Test
  void legacyRootDigestIsStableAndDoesNotClaimResultCapability() {
    TransactionId transaction = new TransactionId(UUID.fromString("00000000-0000-0000-0000-000000000001"));
    ManifestRoot root = ManifestRoot.create(transaction, 1, 0, 1, 0, 0, Optional.empty());
    assertEquals("82e21d29e01794fd788b428cc765f2519ae83164cc57486349ea6b6aa551c71b",
        HexFormat.of().formatHex(root.digest().bytes()));
    assertTrue(root.resultReference().isEmpty());
  }

  @Test
  void resultRootBindsTheEntireResultReferenceAndAttempt() {
    ResultIdentity identity = identity();
    ResultPage page = new ResultPage(identity, 0, new byte[] {1});
    ResultReference result = ResultDirectory.create(identity, List.of(ResultPageReference.from(page))).reference();
    ManifestRoot root = root(identity, result);
    assertEquals(result, root.resultReference().orElseThrow());
    assertNotEquals(root.digest(), ManifestRoot.create(identity.transactionId(), 1, 0, 1, 0, 0,
        Optional.empty()).digest());
    assertEquals(root.digest(), ManifestRoot.restoreWithResult(identity.transactionId(), 1, 0, 1,
        0, 0, Optional.empty(), 0, 0, Optional.empty(), result, root.digest()).digest());
    ResultReference different = ResultDirectory.create(identity, List.of(ResultPageReference.from(
        new ResultPage(identity, 0, new byte[] {2})))).reference();
    assertNotEquals(root.digest(), root(identity, different).digest());
    assertThrows(IllegalArgumentException.class, () -> ManifestRoot.restoreWithResult(
        identity.transactionId(), 1, 0, 1, 0, 0, Optional.empty(), 0, 0, Optional.empty(),
        different, root.digest()));
    assertThrows(IllegalArgumentException.class, () -> root(identity(), result));
    assertFalse(root.digest().equals(different.digest()));
  }

  private static ManifestRoot root(ResultIdentity identity, ResultReference result) {
    return ManifestRoot.createWithResult(identity.transactionId(), 1, 0, 1, 0, 0,
        Optional.empty(), 0, 0, Optional.empty(), result);
  }

  private static ResultIdentity identity() {
    return ResultIdentity.from(InvocationBinding.begin(InvocationId.create(), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1));
  }
}
