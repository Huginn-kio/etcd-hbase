package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.error.BackendFailure;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import mvccpb.Kv;
import org.junit.jupiter.api.Test;

final class WatchResponseFragmenterTest {
  private static final long MOD_REVISION = 42;

  @Test
  void leavesACompleteRevisionAsOneUnfragmentedResponseWhenItFits() throws BackendFailure {
    Rpc.WatchResponse response = response(List.of(
        event("key-a", "value-a", MOD_REVISION),
        event("key-b", "value-b", MOD_REVISION)));

    var cursor = WatchResponseFragmenter.split(response, response.getSerializedSize());

    assertTrue(cursor.hasNext());
    assertSame(response, cursor.next());
    assertFalse(cursor.hasNext());
    assertThrows(NoSuchElementException.class, cursor::next);
  }

  @Test
  void splitsLargeRevisionInOrderAndMarksOnlyNonFinalFragments() throws BackendFailure {
    List<Kv.Event> events = List.of(
        event("key-a", "x".repeat(48), MOD_REVISION),
        event("key-b", "y".repeat(48), MOD_REVISION),
        event("key-c", "z".repeat(48), MOD_REVISION));
    Rpc.WatchResponse response = response(events);
    int maximumEncodedBytes = responseWithFragment(response, events.getFirst(), true)
        .getSerializedSize();

    List<Rpc.WatchResponse> fragments = drain(
        WatchResponseFragmenter.split(response, maximumEncodedBytes));

    assertEquals(events.size(), fragments.size());
    for (int index = 0; index < fragments.size(); index++) {
      Rpc.WatchResponse fragment = fragments.get(index);
      assertEquals(index < fragments.size() - 1, fragment.getFragment());
      assertEquals(response.getWatchId(), fragment.getWatchId());
      assertEquals(response.getHeader(), fragment.getHeader());
      assertTrue(fragment.getSerializedSize() <= maximumEncodedBytes);
      assertEquals(List.of(events.get(index)), fragment.getEventsList());
    }
  }

  @Test
  void packsMultipleEventsIntoEachFragmentWithoutChangingTheirOrder() throws BackendFailure {
    List<Kv.Event> events = List.of(
        event("key-a", "a".repeat(32), MOD_REVISION),
        event("key-b", "b".repeat(32), MOD_REVISION),
        event("key-c", "c".repeat(32), MOD_REVISION),
        event("key-d", "d".repeat(32), MOD_REVISION));
    Rpc.WatchResponse response = response(events);
    int maximumEncodedBytes = response.toBuilder().clearEvents().setFragment(true)
        .addAllEvents(events.subList(0, 2)).build().getSerializedSize();

    List<Rpc.WatchResponse> fragments = drain(
        WatchResponseFragmenter.split(response, maximumEncodedBytes));

    assertEquals(2, fragments.size());
    assertTrue(fragments.getFirst().getFragment());
    assertEquals(events.subList(0, 2), fragments.getFirst().getEventsList());
    assertFalse(fragments.getLast().getFragment());
    assertEquals(events.subList(2, 4), fragments.getLast().getEventsList());
    assertEquals(events, fragments.stream().flatMap(fragment -> fragment.getEventsList().stream())
        .toList());
    assertTrue(fragments.stream().allMatch(
        fragment -> fragment.getSerializedSize() <= maximumEncodedBytes));
  }

  @Test
  void allowsTheFinalFragmentToUseItsSmallerFalseFlagEncoding() throws BackendFailure {
    Kv.Event firstEvent = event("key-a", "short", MOD_REVISION);
    Kv.Event lastEvent = event("key-b", "l".repeat(96), MOD_REVISION);
    Rpc.WatchResponse response = response(List.of(firstEvent, lastEvent));
    int maximumEncodedBytes = responseWithFragment(response, lastEvent, false)
        .getSerializedSize();

    List<Rpc.WatchResponse> fragments = drain(
        WatchResponseFragmenter.split(response, maximumEncodedBytes));

    assertEquals(2, fragments.size());
    assertTrue(fragments.getFirst().getFragment());
    assertEquals(List.of(firstEvent), fragments.getFirst().getEventsList());
    assertFalse(fragments.getLast().getFragment());
    assertEquals(List.of(lastEvent), fragments.getLast().getEventsList());
    assertEquals(maximumEncodedBytes, fragments.getLast().getSerializedSize());
  }

  @Test
  void includesPrevKvAndProtoOverheadWhenRejectingAnOversizedEvent() {
    Kv.Event event = event("key-a", "new-value", MOD_REVISION)
        .toBuilder()
        .setPrevKv(keyValue("key-a", "old-value-that-is-larger", MOD_REVISION - 1))
        .build();
    Rpc.WatchResponse response = response(List.of(event));
    int currentOnlyBudget = response(List.of(
        event("key-a", "new-value", MOD_REVISION))).getSerializedSize();
    assertTrue(response.getSerializedSize() > currentOnlyBudget);

    BackendFailure failure = assertThrows(BackendFailure.class,
        () -> WatchResponseFragmenter.split(response, currentOnlyBudget));

    assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
  }

  @Test
  void passesControlResponsesWithoutFragmentingThem() throws BackendFailure {
    Rpc.WatchResponse response = Rpc.WatchResponse.newBuilder()
        .setHeader(header())
        .setWatchId(9)
        .setCreated(true)
        .build();

    var cursor = WatchResponseFragmenter.split(response, response.getSerializedSize());

    assertSame(response, cursor.next());
    assertFalse(cursor.hasNext());
  }

  @Test
  void rejectsControlResponseThatExceedsTheMessageBudget() {
    Rpc.WatchResponse response = Rpc.WatchResponse.newBuilder()
        .setHeader(header())
        .setWatchId(9)
        .setCreated(true)
        .build();

    BackendFailure failure = assertThrows(BackendFailure.class,
        () -> WatchResponseFragmenter.split(response, response.getSerializedSize() - 1));

    assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
  }

  @Test
  void rejectsEventsFromDifferentRevisions() {
    Rpc.WatchResponse response = response(List.of(
        event("key-a", "value-a", MOD_REVISION),
        event("key-b", "value-b", MOD_REVISION + 1)));

    assertThrows(IllegalArgumentException.class,
        () -> WatchResponseFragmenter.split(response, 1024));
  }

  @Test
  void rejectsAlreadyFragmentedInputAndNonPositiveBudget() {
    Rpc.WatchResponse response = response(List.of(event("key-a", "value-a", MOD_REVISION)));
    Rpc.WatchResponse fragment = response.toBuilder().setFragment(true).build();

    assertThrows(IllegalArgumentException.class,
        () -> WatchResponseFragmenter.split(fragment, 1024));
    assertThrows(IllegalArgumentException.class,
        () -> WatchResponseFragmenter.split(response, 0));
  }

  private static List<Rpc.WatchResponse> drain(WatchResponseFragmenter.Cursor cursor) {
    List<Rpc.WatchResponse> responses = new ArrayList<>();
    while (cursor.hasNext()) responses.add(cursor.next());
    return List.copyOf(responses);
  }

  private static Rpc.WatchResponse response(List<Kv.Event> events) {
    return Rpc.WatchResponse.newBuilder()
        .setHeader(header())
        .setWatchId(9)
        .addAllEvents(events)
        .build();
  }

  private static Rpc.WatchResponse responseWithFragment(Rpc.WatchResponse response,
      Kv.Event event, boolean hasMoreFragments) {
    return response.toBuilder().clearEvents().setFragment(hasMoreFragments).addEvents(event).build();
  }

  private static Rpc.ResponseHeader header() {
    return Rpc.ResponseHeader.newBuilder()
        .setClusterId(11)
        .setMemberId(12)
        .setRevision(MOD_REVISION)
        .setRaftTerm(13)
        .build();
  }

  private static Kv.Event event(String key, String value, long modRevision) {
    return Kv.Event.newBuilder()
        .setType(Kv.Event.EventType.PUT)
        .setKv(keyValue(key, value, modRevision))
        .build();
  }

  private static Kv.KeyValue keyValue(String key, String value, long modRevision) {
    return Kv.KeyValue.newBuilder()
        .setKey(ByteString.copyFromUtf8(key))
        .setValue(ByteString.copyFromUtf8(value))
        .setCreateRevision(1)
        .setModRevision(modRevision)
        .setVersion(1)
        .build();
  }
}
