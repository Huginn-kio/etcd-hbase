package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.KeyRange;
import java.util.Objects;

/** A typed comparison over one logical key range, independent of the wire representation. */
public final class CompareCondition {
  public enum Target { VERSION, CREATE, MOD, VALUE, LEASE, UNRECOGNIZED }
  public enum Relation { EQUAL, NOT_EQUAL, GREATER, LESS, UNRECOGNIZED }

  private final KeyRange range;
  private final Target target;
  private final Relation relation;
  private final long number;
  private final byte[] value;

  private CompareCondition(KeyRange range, Target target, Relation relation, long number, byte[] value) {
    this.range = Objects.requireNonNull(range, "range");
    this.target = Objects.requireNonNull(target, "target");
    this.relation = Objects.requireNonNull(relation, "relation");
    this.number = number;
    this.value = value == null ? null : value.clone();
  }

  public static CompareCondition numeric(KeyRange range, Target target, Relation relation, long expected) {
    if (target == Target.VALUE) throw new IllegalArgumentException("VALUE requires a byte operand");
    return new CompareCondition(range, target, relation, expected, null);
  }

  public static CompareCondition value(KeyRange range, Relation relation, byte[] expected) {
    return new CompareCondition(range, Target.VALUE, relation, 0, Objects.requireNonNull(expected, "expected"));
  }

  public KeyRange range() { return range; }
  public Target target() { return target; }
  public Relation relation() { return relation; }
  long number() { return number; }
  byte[] value() { return value == null ? null : value.clone(); }
}
