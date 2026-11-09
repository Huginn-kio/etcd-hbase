package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultPageReference;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.kv.PutResult;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.ResultTree;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ResultMessagesTest {
  @Test
  void roundTripsPutPresenceMetadataAndBinaryValue() {
    ByteKey key = ByteKey.copyOf(new byte[] {0, (byte) 0xff});
    StorageMetadata metadata = new StorageMetadata(1, 2, 3, -42,
        StorageMetadata.State.LIVE, 0);
    PutResult original = new PutResult(key, 3,
        Optional.of(StoredRecord.live(key, metadata, new byte[0])));
    PutResult restored = ResultMessages.decodePut(ResultMessages.encode(original));
    var buffer = java.nio.ByteBuffer.wrap(ResultMessages.encode(original)).asReadOnlyBuffer();
    assertEquals(original.revision(), ResultMessages.decodePut(buffer).revision());
    assertEquals(0, buffer.position());
    assertEquals(key, restored.key());
    assertEquals(3, restored.revision());
    assertEquals(metadata, restored.previous().orElseThrow().metadata());
    assertArrayEquals(new byte[0], restored.previous().orElseThrow().value().orElseThrow());
    assertTrue(ResultMessages.decodePut(ResultMessages.encode(
        new PutResult(key, 3, Optional.empty()))).previous().isEmpty());
  }

  @Test
  void roundTripsPagesAndDirectoryAndRejectsTamperedPage() throws Exception {
    ResultIdentity identity = identity();
    ResultPage page = new ResultPage(identity, 0, new byte[] {1, 2});
    ResultDirectory directory = ResultDirectory.create(identity, List.of(ResultPageReference.from(page)));
    ResultPage restoredPage = ResultMessages.decodePage(ResultMessages.encode(page));
    ResultDirectory restoredDirectory = ResultMessages.decodeDirectory(ResultMessages.encode(directory));
    assertTrue(ResultPageReference.from(page).matches(restoredPage));
    assertEquals(directory.reference(), restoredDirectory.reference());

    var modified = CommitProtos.ResultPage.parseFrom(ResultMessages.encode(page)).toBuilder()
        .setPayload(ByteString.copyFrom(new byte[] {9})).build().toByteArray();
    assertThrows(IllegalArgumentException.class, () -> ResultMessages.decodePage(modified));
    var badCount = CommitProtos.ResultPage.parseFrom(ResultMessages.encode(page)).toBuilder()
        .setItemCount(0).build().toByteArray();
    assertThrows(IllegalArgumentException.class, () -> ResultMessages.decodePage(badCount));
    assertThrows(IllegalArgumentException.class,
        () -> ResultMessages.decodeDirectory(new byte[ResultMessages.MAX_DIRECTORY_BYTES + 1]));
  }

  @Test
  void rejectsIncompletePayloadAndUnknownOrMismatchedVersion() throws Exception {
    ByteKey key = ByteKey.copyOf(new byte[] {'k'});
    byte[] encoded = ResultMessages.encode(new PutResult(key, 2, Optional.empty()));
    var message = CommitProtos.ResultTree.parseFrom(encoded);
    assertThrows(IllegalArgumentException.class, () -> ResultMessages.decodePut(
        message.toBuilder().clearRoot().build().toByteArray()));
    assertThrows(IllegalArgumentException.class, () -> ResultMessages.decodePut(
        message.toBuilder().setFormatVersion(99).build().toByteArray()));
    byte[] unknown = java.util.Arrays.copyOf(encoded, encoded.length + 3);
    unknown[encoded.length] = (byte) 0x98;
    unknown[encoded.length + 1] = 0x06;
    unknown[encoded.length + 2] = 0x01;
    assertThrows(IllegalArgumentException.class, () -> ResultMessages.decodePut(unknown));
    assertFalse(message.getRoot().getPut().hasPrevious());
  }

  @Test
  void rootVersionDistinguishesLegacyFromResultBearingRecords() throws Exception {
    ResultIdentity identity = identity();
    ResultPage page = new ResultPage(identity, 0, new byte[] {1});
    var reference = ResultDirectory.create(identity, List.of(ResultPageReference.from(page))).reference();
    ManifestRoot legacy = ManifestRoot.create(identity.transactionId(), 1, 0, 1, 0, 0, Optional.empty());
    ManifestRoot resultRoot = ManifestRoot.createWithResult(identity.transactionId(), 1, 0, 1,
        0, 0, Optional.empty(), 0, 0, Optional.empty(), reference);
    var oldMessage = CommitProtos.ManifestRoot.parseFrom(ManifestMessages.encode(legacy));
    var newMessage = CommitProtos.ManifestRoot.parseFrom(ManifestMessages.encode(resultRoot));
    assertEquals(2, oldMessage.getFormatVersion());
    assertEquals(3, newMessage.getFormatVersion());
    assertEquals(legacy.digest(), ManifestMessages.decodeRoot(oldMessage.toByteArray()).digest());
    assertEquals(reference, ManifestMessages.decodeRoot(newMessage.toByteArray()).resultReference().orElseThrow());
    assertThrows(IllegalArgumentException.class,
        () -> ManifestMessages.decodeRoot(newMessage.toBuilder().clearResult().build().toByteArray()));
    assertThrows(IllegalArgumentException.class,
        () -> ManifestMessages.decodeRoot(newMessage.toBuilder().setFormatVersion(2).build().toByteArray()));
    assertThrows(IllegalArgumentException.class,
        () -> ManifestMessages.decodeRoot(oldMessage.toBuilder().setFormatVersion(3).build().toByteArray()));
  }

  @Test
  void roundTripsNestedLogicalResultWithIndependentHeadersAndProjectionPresence() throws Exception {
    ByteKey key = ByteKey.copyOf(new byte[] {0, (byte) 0xff});
    var metadata = new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, 0);
    StoredRecord empty = StoredRecord.live(key, metadata, new byte[0]);
    StoredRecord keysOnly = StoredRecord.metadataOnly(key,
        new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, 7));
    var absent = new ResultNode.Header(false, 0);
    var current = new ResultNode.Header(true, 3);
    ResultTree tree = new ResultTree(new ResultNode.Txn(0, current, false, List.of(
        new ResultNode.Range(0, absent, List.of(keysOnly), 1, true),
        new ResultNode.Txn(1, absent, true, List.of(
            new ResultNode.Put(0, current, key, Optional.of(empty)),
            new ResultNode.Delete(1, absent, 1, List.of(empty)))))));
    ResultTree restored = ResultMessages.decodeTree(java.nio.ByteBuffer.wrap(ResultMessages.encode(tree)));
    assertEquals(5, restored.nodeCount());
    assertEquals(3, restored.recordCount());
    var outer = (ResultNode.Txn) restored.root();
    assertFalse(outer.succeeded());
    var range = (ResultNode.Range) outer.responses().getFirst();
    assertFalse(range.header().present());
    assertTrue(range.more());
    assertFalse(range.records().getFirst().hasValue());
    assertEquals(7, range.records().getFirst().metadata().valueLength());
    var nested = (ResultNode.Txn) outer.responses().get(1);
    var put = (ResultNode.Put) nested.responses().getFirst();
    assertEquals(3, put.header().revision());
    assertArrayEquals(new byte[0], put.previous().orElseThrow().value().orElseThrow());
    assertEquals(1, ((ResultNode.Delete) nested.responses().get(1)).deleted());
    var encoded = CommitProtos.ResultTree.parseFrom(ResultMessages.encode(tree));
    assertThrows(IllegalArgumentException.class, () -> ResultMessages.decodeTree(java.nio.ByteBuffer.wrap(
        encoded.toBuilder().setNodeCount(encoded.getNodeCount() + 1).build().toByteArray())));
  }

  @Test
  void rejectsMissingTreeVariantInvalidOrdinalAndExcessDepth() throws Exception {
    var header = CommitProtos.ResultHeader.newBuilder().setPresent(false).setRevision(0).build();
    var emptyNode = CommitProtos.ResultNode.newBuilder().setOrdinal(0).setHeader(header).build();
    var invalid = CommitProtos.ResultTree.newBuilder().setFormatVersion(1).setRoot(emptyNode).build();
    assertThrows(IllegalArgumentException.class,
        () -> ResultMessages.decodeTree(java.nio.ByteBuffer.wrap(invalid.toByteArray())));
    var wrongOrdinal = new ResultNode.Put(1, new ResultNode.Header(false, 0),
        ByteKey.copyOf(new byte[] {'k'}), Optional.empty());
    assertThrows(IllegalArgumentException.class, () -> new ResultTree(wrongOrdinal));
    ResultNode node = new ResultNode.Put(0, new ResultNode.Header(false, 0),
        ByteKey.copyOf(new byte[] {'k'}), Optional.empty());
    for (int index = 0; index < ResultTree.MAX_DEPTH; index++) {
      node = new ResultNode.Txn(0, new ResultNode.Header(false, 0), true, List.of(node));
    }
    ResultNode tooDeep = node;
    assertThrows(IllegalArgumentException.class, () -> new ResultTree(tooDeep));
  }

  private static ResultIdentity identity() {
    return ResultIdentity.from(InvocationBinding.begin(InvocationId.create(),
        ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), TransactionId.create(),
        1, 0, 1));
  }
}
