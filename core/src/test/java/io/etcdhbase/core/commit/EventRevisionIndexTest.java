package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class EventRevisionIndexTest {
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);
  private static final ByteKey KEY = ByteKey.copyOf(new byte[] {1});

  @Test
  void indexMatchesTheSelectedDecisionAndSealedEventRoot() {
    ManifestRoot root = root(TransactionId.create(), true);
    CommitDecision decision = decision(root, 3);

    EventRevisionIndex index = EventRevisionIndex.fromSelected(decision, root);

    assertEquals(decision, index.decision());
    assertEquals(root.eventDirectory().orElseThrow(), index.events());
    index.verifySelected(decision, root);
  }

  @Test
  void rejectsMissingEventsAndDecisionOrManifestMismatch() {
    TransactionId transaction = TransactionId.create();
    ManifestRoot root = root(transaction, true);
    CommitDecision decision = decision(root, 3);
    EventRevisionIndex index = EventRevisionIndex.fromSelected(decision, root);
    ManifestRoot missingEvents = root(transaction, false);

    assertThrows(IllegalStateException.class,
        () -> EventRevisionIndex.fromSelected(decision(missingEvents, 3), missingEvents));
    assertThrows(IllegalStateException.class,
        () -> EventRevisionIndex.fromSelected(decision(root, 4), root));
    assertThrows(IllegalStateException.class,
        () -> index.verifySelected(decision, root(TransactionId.create(), true)));
    var differentEvents = new EventReference(ByteKey.copyOf(new byte[] {2}), DIGEST,
        0, 0, 1, 1, 1, 0);
    assertThrows(IllegalStateException.class,
        () -> new EventRevisionIndex(decision, differentEvents).verifySelected(decision, root));
  }

  @Test
  void metadataOnlyDecisionsDoNotProduceEventIndexes() {
    ManifestRoot root = ManifestRoot.create(TransactionId.create(), 1, 2, 10, 0, 0,
        Optional.empty());
    CommitDecision metadata = new CommitDecision(root.transactionId(), InvocationId.create(), 3,
        10, 10, root.systemIdentity(), root.digest(), false);
    assertThrows(IllegalStateException.class,
        () -> EventRevisionIndex.fromSelected(metadata, root));
  }

  private static ManifestRoot root(TransactionId transaction, boolean withEvent) {
    var candidates = new ManifestChildReference(KEY, DIGEST, 0, 0, KEY, KEY, 1, 0, 1, 0);
    var event = new EventReference(KEY, DIGEST, 0, 0, 1, 1, 1, 0);
    return ManifestRoot.create(transaction, 1, 2, 10, 1, 0, Optional.of(candidates),
        withEvent ? 1 : 0, withEvent ? 1 : 0, withEvent ? Optional.of(event) : Optional.empty());
  }

  private static CommitDecision decision(ManifestRoot root, long sequence) {
    return new CommitDecision(root.transactionId(), InvocationId.create(), sequence, 10, 11,
        root.systemIdentity(), root.digest(), true);
  }
}
