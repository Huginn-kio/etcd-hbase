package io.etcdhbase.hbase.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultPageReference;
import io.etcdhbase.core.commit.ResultReference;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.kv.PutResult;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.ResultTree;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Versioned internal codecs for recoverable, endpoint-independent Put responses. */
public final class ResultMessages {
  private static final int FORMAT_VERSION = 1;
  public static final int MAX_DIRECTORY_BYTES = 64 * 1024;
  public static final int MAX_PAGE_RECORD_BYTES = ResultPage.MAX_PAYLOAD_BYTES + 1024;
  public static final int MAX_RESULT_BYTES = ResultDirectory.MAX_PAGES * ResultPage.MAX_PAYLOAD_BYTES;

  private ResultMessages() {}

  public static byte[] encode(PutResult result) {
    return encode(new ResultTree(new ResultNode.Put(0, new ResultNode.Header(true, result.revision()),
        result.key(), result.previous())));
  }

  public static PutResult decodePut(byte[] encoded) {
    return decodePut(ByteBuffer.wrap(encoded));
  }

  /** Decodes the retained result without advancing the caller's read-only buffer view. */
  public static PutResult decodePut(ByteBuffer encoded) {
    ResultNode root = decodeTree(encoded).root();
    if (!(root instanceof ResultNode.Put put) || !put.header().present() || put.header().revision() <= 0) {
      throw new IllegalArgumentException("persisted result is not a standalone Put response");
    }
    return new PutResult(put.key(), put.header().revision(), put.previous());
  }

  public static byte[] encode(ResultTree tree) {
    var message = CommitProtos.ResultTree.newBuilder().setFormatVersion(FORMAT_VERSION)
        .setRoot(encode(tree.root())).setNodeCount(tree.nodeCount()).setRecordCount(tree.recordCount())
        .setRetainedBytes(tree.retainedBytes()).build();
    requireLength(message.getSerializedSize(), MAX_RESULT_BYTES);
    return message.toByteArray();
  }

  public static ResultTree decodeTree(ByteBuffer encoded) {
    requireLength(encoded.remaining(), MAX_RESULT_BYTES);
    try {
      var input = com.google.protobuf.CodedInputStream.newInstance(encoded.asReadOnlyBuffer());
      input.setRecursionLimit(ResultTree.MAX_DEPTH * 2 + 8);
      input.setSizeLimit(MAX_RESULT_BYTES);
      var message = CommitProtos.ResultTree.parseFrom(input);
      if (!message.getUnknownFields().asMap().isEmpty() || !message.hasFormatVersion()
          || message.getFormatVersion() != FORMAT_VERSION || !message.hasRoot()
          || !message.hasNodeCount() || !message.hasRecordCount() || !message.hasRetainedBytes()) throw invalid();
      ResultTree tree = new ResultTree(decodeNode(message.getRoot(), 1, new int[] {0, 0}));
      if (tree.nodeCount() != message.getNodeCount() || tree.recordCount() != message.getRecordCount()
          || tree.retainedBytes() != message.getRetainedBytes()) throw invalid();
      return tree;
    } catch (java.io.IOException | IllegalArgumentException | ArithmeticException failure) {
      throw new IllegalArgumentException("invalid persisted logical result tree", failure);
    }
  }

  private static CommitProtos.ResultNode encode(ResultNode node) {
    var header = CommitProtos.ResultHeader.newBuilder().setPresent(node.header().present())
        .setRevision(node.header().revision()).build();
    var builder = CommitProtos.ResultNode.newBuilder().setOrdinal(node.ordinal()).setHeader(header);
    switch (node) {
      case ResultNode.Put put -> {
        var value = CommitProtos.PutResultNode.newBuilder().setKey(bytes(put.key()));
        put.previous().ifPresent(record -> value.setPrevious(encode(record)));
        builder.setPut(value);
      }
      case ResultNode.Delete delete -> {
        var value = CommitProtos.DeleteResultNode.newBuilder().setDeleted(delete.deleted());
        delete.previous().forEach(record -> value.addPrevious(encode(record)));
        builder.setDelete(value);
      }
      case ResultNode.Range range -> {
        var value = CommitProtos.RangeResultNode.newBuilder().setCount(range.count()).setMore(range.more());
        range.records().forEach(record -> value.addRecords(encode(record)));
        builder.setRange(value);
      }
      case ResultNode.Txn txn -> {
        var value = CommitProtos.TxnResultNode.newBuilder().setSucceeded(txn.succeeded());
        txn.responses().forEach(response -> value.addResponses(encode(response)));
        builder.setTxn(value);
      }
    }
    return builder.build();
  }

  private static ResultNode decodeNode(CommitProtos.ResultNode message, int depth, int[] count) {
    if (depth > ResultTree.MAX_DEPTH || ++count[0] > ResultTree.MAX_NODES
        || !message.getUnknownFields().asMap().isEmpty() || !message.hasOrdinal()
        || !message.hasHeader() || !message.getHeader().getUnknownFields().asMap().isEmpty()
        || !message.getHeader().hasPresent() || !message.getHeader().hasRevision()) throw invalid();
    ResultNode.Header header = new ResultNode.Header(message.getHeader().getPresent(),
        message.getHeader().getRevision());
    int ordinal = message.getOrdinal();
    return switch (message.getKindCase()) {
      case PUT -> {
        var put = message.getPut();
        if (!put.getUnknownFields().asMap().isEmpty() || !put.hasKey()) throw invalid();
        addRecords(count, put.hasPrevious() ? 1 : 0);
        yield new ResultNode.Put(ordinal, header, key(put.getKey()), put.hasPrevious()
            ? Optional.of(decode(put.getPrevious(), true)) : Optional.empty());
      }
      case DELETE -> {
        var delete = message.getDelete();
        if (!delete.getUnknownFields().asMap().isEmpty() || !delete.hasDeleted()
            || delete.getPreviousCount() > ResultNode.MAX_RECORDS_PER_NODE) throw invalid();
        addRecords(count, delete.getPreviousCount());
        List<StoredRecord> records = new ArrayList<>(delete.getPreviousCount());
        for (var record : delete.getPreviousList()) records.add(decode(record, true));
        yield new ResultNode.Delete(ordinal, header, delete.getDeleted(), records);
      }
      case RANGE -> {
        var range = message.getRange();
        if (!range.getUnknownFields().asMap().isEmpty() || !range.hasCount() || !range.hasMore()
            || range.getRecordsCount() > ResultNode.MAX_RECORDS_PER_NODE) throw invalid();
        addRecords(count, range.getRecordsCount());
        List<StoredRecord> records = new ArrayList<>(range.getRecordsCount());
        for (var record : range.getRecordsList()) records.add(decode(record, false));
        yield new ResultNode.Range(ordinal, header, records, range.getCount(), range.getMore());
      }
      case TXN -> {
        var txn = message.getTxn();
        if (!txn.getUnknownFields().asMap().isEmpty() || !txn.hasSucceeded()
            || txn.getResponsesCount() > ResultNode.MAX_OPERATIONS) throw invalid();
        List<ResultNode> children = new ArrayList<>(txn.getResponsesCount());
        for (var child : txn.getResponsesList()) children.add(decodeNode(child, depth + 1, count));
        yield new ResultNode.Txn(ordinal, header, txn.getSucceeded(), children);
      }
      case KIND_NOT_SET -> throw invalid();
    };
  }

  private static void addRecords(int[] count, int amount) {
    count[1] = Math.addExact(count[1], amount);
    if (count[1] > ResultTree.MAX_RECORDS) throw invalid();
  }

  public static byte[] encode(ResultPage page) {
    byte[] encoded = CommitProtos.ResultPage.newBuilder().setFormatVersion(FORMAT_VERSION)
        .setIdentity(encode(page.identity())).setPageIndex(page.index())
        .setPayload(ByteString.copyFrom(page.payload())).setDigest(bytes(page.digest())).setItemCount(page.itemCount())
        .build().toByteArray();
    requireLength(encoded.length, MAX_PAGE_RECORD_BYTES);
    return encoded;
  }

  public static ResultPage decodePage(byte[] encoded) {
    requireLength(encoded.length, MAX_PAGE_RECORD_BYTES);
    try {
      var message = CommitProtos.ResultPage.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasIdentity() || !message.hasPageIndex() || !message.hasPayload()
          || !message.hasDigest() || !message.hasItemCount()) throw invalid();
      ResultPage page = ResultPage.restore(decode(message.getIdentity()), message.getPageIndex(),
          message.getPayload().toByteArray(), key(message.getDigest()));
      if (page.itemCount() != message.getItemCount()) throw invalid();
      return page;
    } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
      throw new IllegalArgumentException("invalid persisted result page", failure);
    }
  }

  public static byte[] encode(ResultDirectory directory) {
    var builder = CommitProtos.ResultDirectory.newBuilder().setFormatVersion(FORMAT_VERSION)
        .setIdentity(encode(directory.identity())).setDigest(bytes(directory.digest()))
        .setItemCount(directory.itemCount());
    for (ResultPageReference page : directory.pages()) {
      builder.addPages(CommitProtos.ResultPageReference.newBuilder().setPageIndex(page.index())
          .setPayloadBytes(page.payloadBytes()).setItemCount(page.itemCount()).setDigest(bytes(page.digest())));
    }
    byte[] encoded = builder.build().toByteArray();
    requireLength(encoded.length, MAX_DIRECTORY_BYTES);
    return encoded;
  }

  public static ResultDirectory decodeDirectory(byte[] encoded) {
    requireLength(encoded.length, MAX_DIRECTORY_BYTES);
    try {
      var message = CommitProtos.ResultDirectory.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasIdentity() || !message.hasDigest() || !message.hasItemCount()
          || message.getItemCount() != 1
          || message.getPagesCount() > ResultDirectory.MAX_PAGES) throw invalid();
      ResultIdentity identity = decode(message.getIdentity());
      List<ResultPageReference> pages = new ArrayList<>(message.getPagesCount());
      for (var page : message.getPagesList()) {
        if (!page.getUnknownFields().asMap().isEmpty() || !page.hasPageIndex()
            || !page.hasPayloadBytes() || !page.hasItemCount() || !page.hasDigest()) throw invalid();
        pages.add(new ResultPageReference(identity, page.getPageIndex(), page.getPayloadBytes(),
            page.getItemCount(), key(page.getDigest())));
      }
      return ResultDirectory.restore(identity, pages, key(message.getDigest()));
    } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
      throw new IllegalArgumentException("invalid persisted result directory", failure);
    }
  }

  static CommitProtos.ResultReference encode(ResultReference reference) {
    return CommitProtos.ResultReference.newBuilder().setIdentity(encode(reference.identity()))
        .setDigest(bytes(reference.digest())).setPageCount(reference.pageCount())
        .setItemCount(reference.itemCount()).setTotalBytes(reference.totalBytes()).build();
  }

  static ResultReference decode(CommitProtos.ResultReference message) {
    if (!message.getUnknownFields().asMap().isEmpty() || !message.hasIdentity()
        || !message.hasDigest() || !message.hasPageCount() || !message.hasItemCount()
        || !message.hasTotalBytes()) throw invalid();
    return new ResultReference(decode(message.getIdentity()), key(message.getDigest()),
        message.getPageCount(), message.getItemCount(), message.getTotalBytes());
  }

  private static CommitProtos.ResultIdentity encode(ResultIdentity identity) {
    return CommitProtos.ResultIdentity.newBuilder()
        .setInvocationId(ByteString.copyFrom(identity.invocationId().bytes()))
        .setTransactionId(ByteString.copyFrom(identity.transactionId().bytes()))
        .setRequestDigest(bytes(identity.requestDigest()))
        .setAuthorizationScopeDigest(bytes(identity.authorizationScopeDigest()))
        .setOwnerEpoch(identity.ownerEpoch()).setBaseSequence(identity.baseSequence())
        .setBaseRevision(identity.baseRevision()).build();
  }

  private static ResultIdentity decode(CommitProtos.ResultIdentity message) {
    if (!message.getUnknownFields().asMap().isEmpty() || !message.hasInvocationId()
        || !message.hasTransactionId() || !message.hasRequestDigest()
        || !message.hasAuthorizationScopeDigest() || !message.hasOwnerEpoch()
        || !message.hasBaseSequence() || !message.hasBaseRevision()) throw invalid();
    return new ResultIdentity(InvocationId.fromBytes(message.getInvocationId().toByteArray()),
        TransactionId.fromBytes(message.getTransactionId().toByteArray()),
        key(message.getRequestDigest()), key(message.getAuthorizationScopeDigest()),
        message.getOwnerEpoch(), message.getBaseSequence(), message.getBaseRevision());
  }

  private static CommitProtos.ResultKeyValue encode(StoredRecord record) {
    StorageMetadata metadata = record.metadata();
    var builder = CommitProtos.ResultKeyValue.newBuilder().setKey(bytes(record.key()))
        .setCreateRevision(metadata.createRevision())
        .setModificationRevision(metadata.modificationRevision()).setVersion(metadata.version())
        .setLeaseId(metadata.leaseId()).setValueLength(metadata.valueLength());
    record.value().ifPresent(value -> builder.setValue(ByteString.copyFrom(value)));
    return builder.build();
  }

  private static StoredRecord decode(CommitProtos.ResultKeyValue message, boolean valueRequired) {
    if (!message.getUnknownFields().asMap().isEmpty() || !message.hasKey()
        || !message.hasCreateRevision() || !message.hasModificationRevision()
        || !message.hasVersion() || !message.hasLeaseId() || !message.hasValueLength()
        || valueRequired && !message.hasValue()) throw invalid();
    byte[] value = message.hasValue() ? message.getValue().toByteArray() : null;
    StorageMetadata metadata = new StorageMetadata(message.getCreateRevision(),
        message.getModificationRevision(), message.getVersion(), message.getLeaseId(),
        StorageMetadata.State.LIVE, message.getValueLength());
    return value == null ? StoredRecord.metadataOnly(key(message.getKey()), metadata)
        : StoredRecord.live(key(message.getKey()), metadata, value);
  }

  private static ByteString bytes(ByteKey key) { return ByteString.copyFrom(key.bytes()); }
  private static ByteKey key(ByteString bytes) { return ByteKey.copyOf(bytes.toByteArray()); }
  private static void requireLength(int size, int maximum) {
    if (size <= 0 || size > maximum) throw invalid();
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("result message violates its storage contract");
  }
}
