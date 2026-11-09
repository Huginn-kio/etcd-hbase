package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestChildReference;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultPageReference;
import io.etcdhbase.core.commit.TransactionId;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ManifestRootMessagesTest {
  @Test
  void resultRootRoundTripsAnEightKiBKeyWithoutChangingLegacyLimit() {
    byte[] bytes = new byte[8192 + 64];
    java.util.Arrays.fill(bytes, (byte) 'k');
    ByteKey key = ByteKey.copyOf(bytes);
    ByteKey digest = ByteKey.copyOf(new byte[32]);
    TransactionId transaction = TransactionId.create();
    var binding = InvocationBinding.begin(InvocationId.create(), digest, digest, transaction, 1, 0, 1);
    var identity = ResultIdentity.from(binding);
    var result = ResultDirectory.create(identity,
        List.of(ResultPageReference.from(new ResultPage(identity, 0, new byte[] {1})))).reference();
    var child = new ManifestChildReference(ByteKey.copyOf(new byte[] {1}), digest,
        0, 0, key, key, 1, 1, 1, 0);
    var root = ManifestRoot.createWithResult(transaction, 1, 0, 1, 1, 1,
        Optional.of(child), 0, 0, Optional.empty(), result);

    byte[] encoded = ManifestMessages.encode(root);
    assertTrue(encoded.length > ManifestMessages.MAX_ROOT_BYTES);
    var restored = ManifestMessages.decodeRoot(encoded);
    assertEquals(root.digest(), restored.digest());
    assertEquals(key, restored.candidateDirectory().orElseThrow().firstKey());
    assertEquals(key, restored.candidateDirectory().orElseThrow().lastKey());
    assertEquals(result, restored.resultReference().orElseThrow());

    var legacy = ManifestRoot.create(transaction, 1, 0, 1, 1, 1, Optional.of(child));
    assertThrows(IllegalArgumentException.class, () -> ManifestMessages.encode(legacy));
    assertThrows(IllegalArgumentException.class,
        () -> ManifestMessages.decodeRoot(new byte[ManifestMessages.MAX_RESULT_ROOT_BYTES + 1]));
  }
}
