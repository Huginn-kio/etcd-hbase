package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/** Indexes points by key and ranges by an augmented AVL interval tree; callers serialize access. */
final class EventSubscriptionIndex<T> {
  private final Map<ByteKey, Set<T>> points = new HashMap<>();
  private final Map<T, Registration<T>> registrations = new HashMap<>();
  private Node<T> ranges;
  private long nextId;

  void add(T subscription, KeyRange range) {
    Registration<T> previous = registrations.remove(subscription);
    if (previous != null) remove(previous);
    if (range.kind() == KeyRange.Kind.EMPTY) return;
    if (range.kind() == KeyRange.Kind.POINT) {
      ByteKey key = range.lowerInclusive().orElseThrow();
      points.computeIfAbsent(key, ignored -> new HashSet<>()).add(subscription);
      registrations.put(subscription, new Registration<>(nextId(), subscription, range));
      return;
    }
    Registration<T> registration = new Registration<>(nextId(), subscription, range);
    registrations.put(subscription, registration);
    ranges = insert(ranges, registration);
  }

  void remove(T subscription, KeyRange range) {
    Registration<T> registration = registrations.get(subscription);
    if (registration == null || !sameRange(registration.range(), range)) return;
    registrations.remove(subscription);
    remove(registration);
  }

  /** Returns the number of interval-tree nodes examined for deterministic complexity tests. */
  int forEachMatch(ByteKey key, Consumer<T> receiver) {
    Set<T> exact = points.get(key);
    if (exact != null) exact.forEach(receiver);
    return visit(ranges, key, receiver);
  }

  private void remove(Registration<T> registration) {
    if (registration.range().kind() == KeyRange.Kind.POINT) {
      ByteKey key = registration.range().lowerInclusive().orElseThrow();
      Set<T> group = points.get(key);
      if (group != null && group.remove(registration.subscription()) && group.isEmpty()) {
        points.remove(key);
      }
    } else {
      ranges = remove(ranges, registration);
    }
  }

  private long nextId() {
    nextId = Math.addExact(nextId, 1);
    return nextId;
  }

  private static <T> Node<T> insert(Node<T> node, Registration<T> registration) {
    if (node == null) return new Node<>(registration);
    int order = compare(registration, node.registration);
    if (order < 0) node.left = insert(node.left, registration);
    else node.right = insert(node.right, registration);
    return balance(node);
  }

  private static <T> Node<T> remove(Node<T> node, Registration<T> registration) {
    if (node == null) return null;
    int order = compare(registration, node.registration);
    if (order < 0) node.left = remove(node.left, registration);
    else if (order > 0) node.right = remove(node.right, registration);
    else {
      if (node.left == null) return node.right;
      if (node.right == null) return node.left;
      Node<T> successor = minimum(node.right);
      node.registration = successor.registration;
      node.right = remove(node.right, successor.registration);
    }
    return balance(node);
  }

  private static <T> Node<T> minimum(Node<T> node) {
    while (node.left != null) node = node.left;
    return node;
  }

  private static <T> Node<T> balance(Node<T> node) {
    refresh(node);
    int factor = height(node.left) - height(node.right);
    if (factor > 1) {
      if (height(node.left.left) < height(node.left.right)) node.left = rotateLeft(node.left);
      return rotateRight(node);
    }
    if (factor < -1) {
      if (height(node.right.right) < height(node.right.left)) node.right = rotateRight(node.right);
      return rotateLeft(node);
    }
    return node;
  }

  private static <T> Node<T> rotateLeft(Node<T> node) {
    Node<T> pivot = node.right;
    node.right = pivot.left;
    pivot.left = node;
    refresh(node);
    return refresh(pivot);
  }

  private static <T> Node<T> rotateRight(Node<T> node) {
    Node<T> pivot = node.left;
    node.left = pivot.right;
    pivot.right = node;
    refresh(node);
    return refresh(pivot);
  }

  private static <T> Node<T> refresh(Node<T> node) {
    node.height = Math.max(height(node.left), height(node.right)) + 1;
    node.maximumUpper = node.registration.range().upperExclusive();
    if (node.left != null) node.maximumUpper = greaterUpper(node.maximumUpper, node.left.maximumUpper);
    if (node.right != null) node.maximumUpper = greaterUpper(node.maximumUpper, node.right.maximumUpper);
    return node;
  }

  private static Optional<ByteKey> greaterUpper(Optional<ByteKey> left, Optional<ByteKey> right) {
    if (left.isEmpty() || right.isEmpty()) return Optional.empty();
    return left.orElseThrow().compareTo(right.orElseThrow()) >= 0 ? left : right;
  }

  private static int height(Node<?> node) {
    return node == null ? 0 : node.height;
  }

  private static <T> int visit(Node<T> node, ByteKey key, Consumer<T> receiver) {
    if (node == null || !upperContains(node.maximumUpper, key)) return 0;
    int visited = 1;
    Registration<T> registration = node.registration;
    if (compareLowerToKey(registration.range().lowerInclusive(), key) > 0) {
      return visited + visit(node.left, key, receiver);
    }
    visited += visit(node.left, key, receiver);
    if (upperContains(registration.range().upperExclusive(), key)) {
      receiver.accept(registration.subscription());
    }
    return visited + visit(node.right, key, receiver);
  }

  private static boolean upperContains(Optional<ByteKey> upper, ByteKey key) {
    return upper.isEmpty() || key.compareTo(upper.orElseThrow()) < 0;
  }

  private static int compare(Registration<?> left, Registration<?> right) {
    int lowerOrder = compareLower(left.range().lowerInclusive(), right.range().lowerInclusive());
    return lowerOrder != 0 ? lowerOrder : Long.compare(left.id(), right.id());
  }

  private static int compareLower(Optional<ByteKey> left, Optional<ByteKey> right) {
    if (left.isEmpty()) return right.isEmpty() ? 0 : -1;
    if (right.isEmpty()) return 1;
    return left.orElseThrow().compareTo(right.orElseThrow());
  }

  private static int compareLowerToKey(Optional<ByteKey> lower, ByteKey key) {
    return lower.isEmpty() ? -1 : lower.orElseThrow().compareTo(key);
  }

  private static boolean sameRange(KeyRange left, KeyRange right) {
    return left.kind() == right.kind()
        && left.lowerInclusive().equals(right.lowerInclusive())
        && left.upperExclusive().equals(right.upperExclusive());
  }

  private record Registration<T>(long id, T subscription, KeyRange range) {}

  private static final class Node<T> {
    private Registration<T> registration;
    private Node<T> left;
    private Node<T> right;
    private int height = 1;
    private Optional<ByteKey> maximumUpper;

    private Node(Registration<T> registration) {
      this.registration = registration;
      maximumUpper = registration.range().upperExclusive();
    }
  }
}
