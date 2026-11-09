package io.etcdhbase.grpc.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.ResultTree;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TxnResponseMapperTest {
  private static final ByteKey KEY = ByteKey.copyOf(new byte[] {0, (byte) 0xff});

  @Test
  void projectsChangedAndEmptyDeletesAtTheirActualOperationRevision() {
    ByteKey a = ByteKey.copyOf(new byte[] {'a'});
    ByteKey b = ByteKey.copyOf(new byte[] {'b'});
    ByteKey z = ByteKey.copyOf(new byte[] {'z'});
    StoredRecord first = StoredRecord.live(a,
        new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, 1), new byte[] {1});
    StoredRecord second = StoredRecord.live(b,
        new StorageMetadata(1, 2, 1, 0, StorageMetadata.State.LIVE, 1), new byte[] {2});
    var command = new TxnCommand(List.of(), List.of(new TxnCommand.PointRange(a),
        new TxnCommand.Delete(KeyRange.between(a, z), true),
        new TxnCommand.Delete(KeyRange.between(a, z), true)), List.of());
    var tree = new ResultTree(new ResultNode.Txn(0, header(3), true, List.of(
        new ResultNode.Range(0, header(2), List.of(first), 1, false),
        new ResultNode.Delete(1, header(3), 2, List.of(first, second)),
        new ResultNode.Delete(2, header(3), 0, List.of()))));
    var response = TxnResponseMapper.toWire(command, tree, 3, true, 1, 2);
    assertEquals(2, response.getResponses(0).getResponseRange().getHeader().getRevision());
    var deleted = response.getResponses(1).getResponseDeleteRange();
    assertEquals(3, deleted.getHeader().getRevision());
    assertEquals(2, deleted.getDeleted());
    assertArrayEquals(a.bytes(), deleted.getPrevKvs(0).getKey().toByteArray());
    assertArrayEquals(b.bytes(), deleted.getPrevKvs(1).getKey().toByteArray());
    assertEquals(0, response.getResponses(2).getResponseDeleteRange().getDeleted());

    var unordered = new ResultTree(new ResultNode.Txn(0, header(3), true, List.of(
        new ResultNode.Range(0, header(2), List.of(first), 1, false),
        new ResultNode.Delete(1, header(3), 2, List.of(second, first)),
        new ResultNode.Delete(2, header(3), 0, List.of()))));
    assertThrows(IllegalArgumentException.class,
        () -> TxnResponseMapper.toWire(command, unordered, 3, true, 1, 2));

    var emptyCommand = new TxnCommand(List.of(), List.of(new TxnCommand.Delete(KeyRange.point(a), false)), List.of());
    var emptyTree = new ResultTree(new ResultNode.Txn(0, header(2), true,
        List.of(new ResultNode.Delete(0, header(2), 0, List.of()))));
    assertEquals(2, TxnResponseMapper.toWire(emptyCommand, emptyTree, 2, false, 1, 2)
        .getResponses(0).getResponseDeleteRange().getHeader().getRevision());
  }

  @Test
  void projectsOuterIdentityAndPerOperationRevisionsWithoutInventingChildIdentities() {
    StoredRecord previous = StoredRecord.live(KEY,
        new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, 0), new byte[0]);
    TxnCommand command = command(true);
    ResultTree result = new ResultTree(new ResultNode.Txn(0, header(3), true, List.of(
        new ResultNode.Range(0, header(2), List.of(previous), 1, false),
        new ResultNode.Put(1, header(3), KEY, Optional.of(previous)))));
    var response = TxnResponseMapper.toWire(command, result, 3, true, Long.MIN_VALUE, 7);
    assertEquals(Long.MIN_VALUE, response.getHeader().getClusterId());
    assertEquals(7, response.getHeader().getMemberId());
    assertEquals(3, response.getHeader().getRevision());
    assertTrue(response.getSucceeded());
    var range = response.getResponses(0).getResponseRange();
    assertTrue(range.hasHeader());
    assertEquals(2, range.getHeader().getRevision());
    assertEquals(0, range.getHeader().getClusterId());
    assertEquals(0, range.getHeader().getMemberId());
    assertArrayEquals(KEY.bytes(), range.getKvs(0).getKey().toByteArray());
    assertArrayEquals(new byte[0], range.getKvs(0).getValue().toByteArray());
    var put = response.getResponses(1).getResponsePut();
    assertEquals(3, put.getHeader().getRevision());
    assertEquals(0, put.getHeader().getClusterId());
    assertTrue(put.hasPrevKv());
    assertEquals(2, put.getPrevKv().getModRevision());
  }

  @Test
  void projectsNestedTxnHeaderAndOrderedChildrenWithoutInventingIdentity() {
    var childCommand = new TxnCommand(List.of(), List.of(new TxnCommand.PointRange(KEY)), List.of());
    var outerCommand = new TxnCommand(List.of(), List.of(
        new TxnCommand.Put(new PutCommand(KEY, new byte[] {1}, 0, false, false, false)),
        new TxnCommand.Nested(childCommand)), List.of());
    var current = StoredRecord.live(KEY,
        new StorageMetadata(3, 3, 1, 0, StorageMetadata.State.LIVE, 1), new byte[] {1});
    var tree = new ResultTree(new ResultNode.Txn(0, header(3), true, List.of(
        new ResultNode.Put(0, header(3), KEY, Optional.empty()),
        new ResultNode.Txn(1, header(0), true,
            List.of(new ResultNode.Range(0, header(3), List.of(current), 1, false))))));
    var outer = TxnResponseMapper.toWire(outerCommand, tree, 3, true, 1, 2);
    var child = outer.getResponses(1).getResponseTxn();
    assertTrue(child.hasHeader());
    assertEquals(0, child.getHeader().getRevision());
    assertEquals(0, child.getHeader().getClusterId());
    assertTrue(child.getSucceeded());
    assertEquals(3, child.getResponses(0).getResponseRange().getHeader().getRevision());
    assertArrayEquals(new byte[] {1}, child.getResponses(0).getResponseRange().getKvs(0).getValue().toByteArray());
  }

  @Test
  void noChangeElseKeepsItsOriginalRevisionAndEmptyResponse() {
    var result = new ResultTree(new ResultNode.Txn(0, header(2), false, List.of()));
    var response = TxnResponseMapper.toWire(command(true), result, 2, false, 1, 2);
    assertFalse(response.getSucceeded());
    assertEquals(2, response.getHeader().getRevision());
    assertEquals(0, response.getResponsesCount());
  }

  @Test
  void refusesAResultThatDoesNotMatchItsRequestRevisionOrIdentity() {
    var result = new ResultTree(new ResultNode.Txn(0, header(2), false, List.of()));
    assertThrows(IllegalArgumentException.class, () -> TxnResponseMapper.toWire(command(true), result, 3, false, 1, 2));
    assertThrows(IllegalArgumentException.class, () -> TxnResponseMapper.toWire(command(true), result, 2, false, 0, 2));
    var wrongBranch = new ResultTree(new ResultNode.Txn(0, header(2), true, List.of()));
    assertThrows(IllegalArgumentException.class,
        () -> TxnResponseMapper.toWire(command(true), wrongBranch, 2, false, 1, 2));
    StoredRecord previous = StoredRecord.live(KEY,
        new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, 0), new byte[0]);
    var inventedPrevious = new ResultTree(new ResultNode.Txn(0, header(3), true, List.of(
        new ResultNode.Range(0, header(2), List.of(previous), 1, false),
        new ResultNode.Put(1, header(3), KEY, Optional.of(previous)))));
    assertThrows(IllegalArgumentException.class,
        () -> TxnResponseMapper.toWire(command(false), inventedPrevious, 3, true, 1, 2));
    var wrongChildRevision = new ResultTree(new ResultNode.Txn(0, header(3), true, List.of(
        new ResultNode.Range(0, header(2), List.of(previous), 1, false),
        new ResultNode.Put(1, header(2), KEY, Optional.of(previous)))));
    assertThrows(IllegalArgumentException.class,
        () -> TxnResponseMapper.toWire(command(true), wrongChildRevision, 3, true, 1, 2));
    assertThrows(IllegalArgumentException.class,
        () -> TxnResponseMapper.toWire(command(true), inventedPrevious, 3, false, 1, 2));
  }

  private static TxnCommand command(boolean previous) {
    return new TxnCommand(List.of(), List.of(new TxnCommand.PointRange(KEY),
        new TxnCommand.Put(new PutCommand(KEY, new byte[] {1}, 0, previous, false, false))), List.of());
  }
  private static ResultNode.Header header(long revision) { return new ResultNode.Header(true, revision); }
}
