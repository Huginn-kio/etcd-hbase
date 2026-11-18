package io.etcdhbase.server.commit;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.grpc.protocol.ResponseHeaderFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import mvccpb.Kv;

/** Collects, filters, projects, and bounds one watch's complete revision batch. */
final class WatchEventBatchEncoder {
  static final int MAX_PENDING_EVENTS_PER_WATCH = 4096;
  private static final String OVERSIZED_RESPONSE_REASON =
      "etcd-hbase: Watch response exceeds the configured message budget";

  record Output(Rpc.WatchResponse response, List<WatchStream.Delivery> deliveries) {
    Output {
      Objects.requireNonNull(response, "response");
      deliveries = List.copyOf(deliveries);
    }

    void release() { deliveries.forEach(WatchStream.Delivery::close); }
  }

  record Poll(Output output, boolean consumed, String terminationReason) {}

  private final long clusterId;
  private final long memberId;
  private final int maximumResponseBytes;

  WatchEventBatchEncoder(long clusterId, long memberId, int maximumResponseBytes) {
    if (clusterId == 0 || memberId == 0 || maximumResponseBytes < 1) {
      throw new IllegalArgumentException("invalid Watch event encoding identity or budget");
    }
    this.clusterId = clusterId;
    this.memberId = memberId;
    this.maximumResponseBytes = maximumResponseBytes;
  }

  Poll nextBatch(WatchRegistry.Watcher watcher) {
    Objects.requireNonNull(watcher, "watcher");
    List<WatchStream.Delivery> deliveries = new ArrayList<>();
    boolean transferred = false;
    try {
      WatchStream.Delivery first = watcher.takeNext();
      if (first == null) {
        String terminationReason = watcher.terminationReason().orElse(null);
        return new Poll(null, terminationReason != null, terminationReason);
      }

      long revision = first.revision();
      deliveries.add(first);
      while (deliveries.size() < MAX_PENDING_EVENTS_PER_WATCH) {
        WatchStream.Delivery next = watcher.pollNext();
        if (next == null) break;
        if (next.revision() != revision) {
          watcher.defer(next);
          break;
        }
        deliveries.add(next);
      }

      Rpc.WatchResponse.Builder response = Rpc.WatchResponse.newBuilder()
          .setHeader(header(revision))
          .setWatchId(watcher.id());
      for (WatchStream.Delivery delivery : deliveries) {
        PreparedEvent event = delivery.event();
        if (delivery.revision() < watcher.minimumEventRevision()
            || event.kind() == PreparedEvent.Kind.PUT && watcher.filterPut()
            || event.kind() == PreparedEvent.Kind.DELETE && watcher.filterDelete()) continue;
        if (event.current().metadata().modificationRevision() != revision) {
          throw new IllegalStateException("Watch event revision differs from its selected revision");
        }
        response.addEvents(project(event, watcher.previousKv()));
      }
      if (response.getEventsCount() == 0) return new Poll(null, true, null);

      Rpc.WatchResponse built = response.build();
      if (built.getSerializedSize() > maximumResponseBytes) {
        return new Poll(null, true, OVERSIZED_RESPONSE_REASON);
      }
      Output output = new Output(built, deliveries);
      transferred = true;
      return new Poll(output, true, null);
    } finally {
      if (!transferred) deliveries.forEach(WatchStream.Delivery::close);
    }
  }

  private Rpc.ResponseHeader header(long revision) {
    return ResponseHeaderFactory.create(clusterId, memberId, revision, 0);
  }

  private static Kv.Event project(PreparedEvent event, boolean previousKv) {
    var builder = Kv.Event.newBuilder()
        .setType(event.kind() == PreparedEvent.Kind.PUT
            ? Kv.Event.EventType.PUT : Kv.Event.EventType.DELETE)
        .setKv(project(event.current()));
    if (previousKv) event.previous().ifPresent(previous -> builder.setPrevKv(project(previous)));
    return builder.build();
  }

  private static Kv.KeyValue project(StoredRecord record) {
    var metadata = record.metadata();
    var builder = Kv.KeyValue.newBuilder().setKey(ByteString.copyFrom(record.key().bytes()))
        .setCreateRevision(metadata.createRevision()).setModRevision(metadata.modificationRevision())
        .setVersion(metadata.version()).setLease(metadata.leaseId());
    record.value().ifPresent(value -> builder.setValue(ByteString.copyFrom(value)));
    return builder.build();
  }
}
