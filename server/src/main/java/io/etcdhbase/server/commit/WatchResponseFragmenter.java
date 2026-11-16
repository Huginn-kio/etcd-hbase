package io.etcdhbase.server.commit;

import com.google.protobuf.CodedOutputStream;
import etcdserverpb.Rpc;
import io.etcdhbase.core.error.BackendFailure;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import mvccpb.Kv;

/** Splits one complete watch revision into wire-size-bounded responses on demand. */
public final class WatchResponseFragmenter {
  private WatchResponseFragmenter() {}

  /**
   * Creates a single-consumer cursor. The caller owns each returned response until its gRPC send
   * and serialization finish; keeping at most one fragment in flight bounds this component's
   * additional memory to one response.
   */
  public static Cursor split(Rpc.WatchResponse response, int maximumEncodedBytes)
      throws BackendFailure {
    Objects.requireNonNull(response, "response");
    if (maximumEncodedBytes <= 0) {
      throw new IllegalArgumentException("maximumEncodedBytes must be positive");
    }
    if (response.getFragment()) {
      throw new IllegalArgumentException("input response is already a fragment");
    }
    if (response.getEventsCount() == 0) {
      if (response.getSerializedSize() > maximumEncodedBytes) {
        throw resourceLimit();
      }
      return Cursor.single(response, maximumEncodedBytes);
    }
    if (response.getCreated() || response.getCanceled() || response.getCompactRevision() != 0) {
      throw new IllegalArgumentException("control response cannot contain watch events");
    }

    List<Kv.Event> events = response.getEventsList();
    long revision = events.getFirst().getKv().getModRevision();
    if (revision <= 0) {
      throw new IllegalArgumentException("watch events must have a positive modification revision");
    }
    for (Kv.Event event : events) {
      if (event.getKv().getModRevision() != revision) {
        throw new IllegalArgumentException("response must contain one complete watch revision");
      }
    }

    int fullResponseBytes = response.getSerializedSize();
    if (fullResponseBytes <= maximumEncodedBytes) {
      return Cursor.single(response, maximumEncodedBytes);
    }

    int finalResponseBaseBytes = response.toBuilder().clearEvents().setFragment(false)
        .build().getSerializedSize();
    int continuedResponseBaseBytes = response.toBuilder().clearEvents().setFragment(true)
        .build().getSerializedSize();
    validateEachEventFits(events, finalResponseBaseBytes, continuedResponseBaseBytes,
        maximumEncodedBytes);
    return Cursor.fragments(response, maximumEncodedBytes, finalResponseBaseBytes,
        continuedResponseBaseBytes);
  }

  private static void validateEachEventFits(List<Kv.Event> events, int finalResponseBaseBytes,
      int continuedResponseBaseBytes, int maximumEncodedBytes) throws BackendFailure {
    int finalEventIndex = events.size() - 1;
    for (int index = 0; index < events.size(); index++) {
      int responseBaseBytes = index == finalEventIndex
          ? finalResponseBaseBytes : continuedResponseBaseBytes;
      long singleEventResponseBytes = responseBaseBytes
          + (long) CodedOutputStream.computeMessageSize(Rpc.WatchResponse.EVENTS_FIELD_NUMBER,
              events.get(index));
      if (singleEventResponseBytes > maximumEncodedBytes) throw resourceLimit();
    }
  }

  private static BackendFailure resourceLimit() {
    return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
  }

  /** Single-consumer cursor. The revision stays ordered and only its final fragment clears the flag. */
  public static final class Cursor {
    private final Rpc.WatchResponse response;
    private final List<Kv.Event> events;
    private final int maximumEncodedBytes;
    private final int finalResponseBaseBytes;
    private final int continuedResponseBaseBytes;
    private int nextEventIndex;
    private boolean singlePending;

    private Cursor(Rpc.WatchResponse response, int maximumEncodedBytes,
        int finalResponseBaseBytes, int continuedResponseBaseBytes, boolean singlePending) {
      this.response = response;
      this.events = response.getEventsList();
      this.maximumEncodedBytes = maximumEncodedBytes;
      this.finalResponseBaseBytes = finalResponseBaseBytes;
      this.continuedResponseBaseBytes = continuedResponseBaseBytes;
      this.singlePending = singlePending;
      this.nextEventIndex = singlePending ? events.size() : 0;
    }

    private static Cursor single(Rpc.WatchResponse response, int maximumEncodedBytes) {
      return new Cursor(response, maximumEncodedBytes, 0, 0, true);
    }

    private static Cursor fragments(Rpc.WatchResponse response, int maximumEncodedBytes,
        int finalResponseBaseBytes, int continuedResponseBaseBytes) {
      return new Cursor(response, maximumEncodedBytes, finalResponseBaseBytes,
          continuedResponseBaseBytes, false);
    }

    public boolean hasNext() {
      return singlePending || nextEventIndex < events.size();
    }

    public Rpc.WatchResponse next() {
      if (singlePending) {
        singlePending = false;
        return response;
      }
      if (!hasNext()) throw new NoSuchElementException("watch response fragments are exhausted");

      int firstEventIndex = nextEventIndex;
      long eventBytes = 0;
      while (nextEventIndex < events.size()) {
        int candidateEventBytes = CodedOutputStream.computeMessageSize(
            Rpc.WatchResponse.EVENTS_FIELD_NUMBER, events.get(nextEventIndex));
        boolean lastEvent = nextEventIndex == events.size() - 1;
        long finalResponseBytes = finalResponseBaseBytes + eventBytes + candidateEventBytes;
        if (lastEvent && finalResponseBytes <= maximumEncodedBytes) {
          nextEventIndex++;
          return build(firstEventIndex, nextEventIndex, false);
        }

        long continuedResponseBytes = continuedResponseBaseBytes + eventBytes + candidateEventBytes;
        if (continuedResponseBytes <= maximumEncodedBytes) {
          eventBytes += candidateEventBytes;
          nextEventIndex++;
          continue;
        }

        if (nextEventIndex == firstEventIndex) {
          throw new IllegalStateException("validated watch event no longer fits its fragment");
        }
        return build(firstEventIndex, nextEventIndex, true);
      }
      throw new IllegalStateException("watch revision ended without a final fragment");
    }

    private Rpc.WatchResponse build(int firstEventIndex, int afterLastEventIndex,
        boolean hasMoreFragments) {
      Rpc.WatchResponse.Builder builder = response.toBuilder().clearEvents()
          .setFragment(hasMoreFragments);
      for (int index = firstEventIndex; index < afterLastEventIndex; index++) {
        builder.addEvents(events.get(index));
      }
      Rpc.WatchResponse fragment = builder.build();
      if (fragment.getSerializedSize() > maximumEncodedBytes) {
        throw new IllegalStateException("watch response exceeded its validated wire-size budget");
      }
      return fragment;
    }
  }
}
