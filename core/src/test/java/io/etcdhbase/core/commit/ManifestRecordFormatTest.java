package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ManifestRecordFormatTest {
  private static final TransactionId TRANSACTION_ID = TransactionId.fromBytes(
      HexFormat.of().parseHex("000102030405060708090a0b0c0d0e0f"));

  @Test
  void canonicalDigestVectorsAndRecordTagsRemainStable() {
    ByteKey candidateDigest = repeated(0x33);
    ByteKey valueDigest = repeated(0x44);
    ManifestCandidateReference candidate = new ManifestCandidateReference(2,
        ByteKey.copyOf(new byte[] {'k', 0}), ByteKey.copyOf(new byte[] {0x11, 0x22}),
        candidateDigest, valueDigest, 3, false);
    ManifestPage manifestPage = ManifestPage.create(TRANSACTION_ID, 7, List.of(candidate));
    assertEquals("c6680e7b14433a0eca1360153d1539a203f41bd795060ce05e0df04d455d9a89",
        hex(manifestPage.digest()));
    assertEquals(ManifestRecordFormat.MANIFEST_PAGE, manifestPage.systemIdentity().bytes()[16]);

    ManifestChildReference manifestChild = new ManifestChildReference(
        ByteKey.copyOf(new byte[] {0x11, 0x22}), repeated(0x33), 1, 1,
        ByteKey.copyOf(new byte[] {'a'}), ByteKey.copyOf(new byte[] {'b'}), 2, 3, 1, 0);
    ManifestDirectoryPage manifestDirectory = ManifestDirectoryPage.create(
        TRANSACTION_ID, 8, 1, List.of(manifestChild));
    assertEquals("fed24abd614d3a03045ae30d8df2384b091f9f3529e5141177edcd1a20cc611f",
        hex(manifestDirectory.digest()));
    assertEquals(ManifestRecordFormat.MANIFEST_DIRECTORY_PAGE,
        manifestDirectory.systemIdentity().bytes()[16]);

    PreparedEventReference event = new PreparedEventReference(7,
        ByteKey.copyOf(new byte[] {0x55, 0x66}), repeated(0x77), 99);
    EventPage eventPage = EventPage.create(TRANSACTION_ID, 9, List.of(event));
    assertEquals("4129ce842ef126b1093398cfb385651ef433d89b9429ac9d01fbb879c7e17d73",
        hex(eventPage.digest()));
    assertEquals(ManifestRecordFormat.EVENT_PAGE, eventPage.systemIdentity().bytes()[16]);

    EventReference eventChild = new EventReference(ByteKey.copyOf(new byte[] {0x55, 0x66}),
        repeated(0x77), 0, 1, 2, 99, 1, 0);
    EventDirectoryPage eventDirectory = EventDirectoryPage.create(
        TRANSACTION_ID, 10, 1, List.of(eventChild));
    assertEquals("38c95ec8d1a01be3a52f1a250a23c3b8d5c418b601f25b1375de7d0ae9d3ef75",
        hex(eventDirectory.digest()));
    assertEquals(ManifestRecordFormat.EVENT_DIRECTORY_PAGE,
        eventDirectory.systemIdentity().bytes()[16]);

    ManifestRoot root = ManifestRoot.create(TRANSACTION_ID, 1, 0, 1, 0, 0, Optional.empty());
    assertEquals("f77537b4f5d418162d4dc4e1030f78d6e1a9d319fd93c5691b8ef10c4510e7eb",
        hex(root.digest()));
    assertEquals(ManifestRecordFormat.MANIFEST_ROOT, root.systemIdentity().bytes()[16]);

    assertEquals(1, ManifestRecordFormat.MANIFEST_PAGE);
    assertEquals(2, ManifestRecordFormat.MANIFEST_DIRECTORY_PAGE);
    assertEquals(3, ManifestRecordFormat.MANIFEST_ROOT);
    assertEquals(4, ManifestRecordFormat.PREPARED_EVENT);
    assertEquals(5, ManifestRecordFormat.EVENT_PAGE);
    assertEquals(6, ManifestRecordFormat.EVENT_DIRECTORY_PAGE);
  }

  @Test
  void restoresRejectEveryAlteredExternalDigest() {
    ByteKey digest = repeated(0x33);
    ManifestCandidateReference candidate = new ManifestCandidateReference(2,
        ByteKey.copyOf(new byte[] {'k'}), ByteKey.copyOf(new byte[] {0x11}),
        digest, repeated(0x44), 3, false);
    ManifestPage page = ManifestPage.create(TRANSACTION_ID, 0, List.of(candidate));
    ManifestChildReference manifestChild = new ManifestChildReference(
        ByteKey.copyOf(new byte[] {0x11}), digest, 1, 1,
        ByteKey.copyOf(new byte[] {'a'}), ByteKey.copyOf(new byte[] {'b'}), 2, 3, 1, 0);
    ManifestDirectoryPage directory = ManifestDirectoryPage.create(
        TRANSACTION_ID, 0, 1, List.of(manifestChild));
    PreparedEventReference event = new PreparedEventReference(0,
        ByteKey.copyOf(new byte[] {0x22}), digest, 99);
    EventPage eventPage = EventPage.create(TRANSACTION_ID, 0, List.of(event));
    EventReference eventChild = new EventReference(ByteKey.copyOf(new byte[] {0x22}),
        digest, 0, 1, 2, 99, 1, 0);
    EventDirectoryPage eventDirectory = EventDirectoryPage.create(
        TRANSACTION_ID, 0, 1, List.of(eventChild));
    ManifestRoot root = ManifestRoot.create(TRANSACTION_ID, 1, 0, 1, 0, 0, Optional.empty());

    assertThrows(IllegalArgumentException.class, () -> ManifestPage.restore(
        TRANSACTION_ID, 0, page.candidates(), changed(page.digest())));
    assertThrows(IllegalArgumentException.class, () -> ManifestDirectoryPage.restore(
        TRANSACTION_ID, 0, 1, directory.children(), changed(directory.digest())));
    assertThrows(IllegalArgumentException.class, () -> EventPage.restore(
        TRANSACTION_ID, 0, eventPage.events(), changed(eventPage.digest())));
    assertThrows(IllegalArgumentException.class, () -> EventDirectoryPage.restore(
        TRANSACTION_ID, 0, 1, eventDirectory.children(), changed(eventDirectory.digest())));
    assertThrows(IllegalArgumentException.class, () -> ManifestRoot.restore(
        TRANSACTION_ID, 1, 0, 1, 0, 0, Optional.empty(), changed(root.digest())));
  }

  private static ByteKey repeated(int value) {
    byte[] bytes = new byte[32];
    java.util.Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }

  private static ByteKey changed(ByteKey digest) {
    byte[] bytes = digest.bytes();
    bytes[0] ^= 1;
    return ByteKey.copyOf(bytes);
  }

  private static String hex(ByteKey value) {
    return HexFormat.of().formatHex(value.bytes());
  }
}
