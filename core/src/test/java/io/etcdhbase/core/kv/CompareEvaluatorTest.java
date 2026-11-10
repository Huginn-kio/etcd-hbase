package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.kv.CompareCondition.Relation;
import io.etcdhbase.core.kv.CompareCondition.Target;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.List;
import org.junit.jupiter.api.Test;

class CompareEvaluatorTest {
  @Test
  void numericTargetsUseSignedInt64ForEveryRelation() {
    var values = List.of(new NumericTarget(Target.VERSION, 3), new NumericTarget(Target.CREATE, 2),
        new NumericTarget(Target.MOD, 5), new NumericTarget(Target.LEASE, -4));
    for (var value : values) {
      for (var relation : Relation.values()) {
        for (long expected : new long[] {value.number() - 1, value.number(), value.number() + 1}) {
          var compare = new CompareEvaluator(CompareCondition.numeric(
              KeyRange.point(key("b")), value.target(), relation, expected));
          compare.accept(live("b", 3, -4, new byte[] {(byte) 0x80}));
          assertEquals(matches(relation, Long.compare(value.number(), expected)), compare.finish(),
              () -> value.target() + " " + relation + " " + expected);
        }
      }
    }
    var extremes = new CompareEvaluator(CompareCondition.numeric(
        KeyRange.point(key("b")), Target.LEASE, Relation.LESS, Long.MAX_VALUE));
    extremes.accept(live("b", 3, Long.MIN_VALUE, new byte[0]));
    assertTrue(extremes.finish());
  }

  @Test
  void valuesCompareUnsignedBytesIncludingEmptyAndHighBytes() {
    for (var relation : Relation.values()) {
      for (int expected : new int[] {0x7f, 0x80, 0xff}) {
        var compare = new CompareEvaluator(CompareCondition.value(KeyRange.point(key("b")),
            relation, new byte[] {(byte) expected}));
        compare.accept(live("b", 3, 0, new byte[] {(byte) 0x80}));
        assertEquals(matches(relation, Integer.compare(0x80, expected)), compare.finish());
      }
    }
    var empty = new CompareEvaluator(CompareCondition.value(KeyRange.point(key("b")), Relation.EQUAL, new byte[0]));
    empty.accept(live("b", 3, 0, new byte[0]));
    assertTrue(empty.finish());
    byte[] operand = new byte[] {1};
    var copied = new CompareEvaluator(CompareCondition.value(KeyRange.point(key("b")), Relation.EQUAL, operand));
    operand[0] = 2;
    copied.accept(live("b", 3, 0, new byte[] {1}));
    assertTrue(copied.finish());
  }

  @Test
  void missingAndTombstonedValuesUseZeroMetadataButNeverMatchValue() {
    for (var relation : Relation.values()) {
      for (var target : List.of(Target.VERSION, Target.CREATE, Target.MOD, Target.LEASE)) {
        var compare = new CompareEvaluator(CompareCondition.numeric(KeyRange.point(key("b")),
            target, relation, 0));
        compare.accept(tombstone("b"));
        assertEquals(matches(relation, 0), compare.finish());
      }
      var value = new CompareEvaluator(CompareCondition.value(KeyRange.point(key("b")),
          relation, new byte[0]));
      assertFalse(value.finish(), "missing value must fail even NOT_EQUAL");
    }
  }

  @Test
  void rangeChecksEveryLiveKeyWithoutRequiringDenseKeys() {
    var condition = CompareCondition.numeric(KeyRange.between(key("a"), key("z")),
        Target.VERSION, Relation.EQUAL, 3);
    var allMatch = new CompareEvaluator(condition);
    allMatch.accept(live("b", 3, 0, new byte[0]));
    allMatch.accept(tombstone("c"));
    allMatch.accept(live("x", 3, 0, new byte[0]));
    assertTrue(allMatch.finish());
    var mismatch = new CompareEvaluator(condition);
    mismatch.accept(live("b", 3, 0, new byte[0]));
    mismatch.accept(live("x", 4, 0, new byte[0]));
    assertFalse(mismatch.finish());
  }

  @Test
  void corruptOrIncompleteInputFailsInsteadOfSelectingElse() {
    var value = new CompareEvaluator(CompareCondition.value(KeyRange.point(key("b")),
        Relation.NOT_EQUAL, new byte[] {1}));
    var metadata = new StorageMetadata(2, 5, 3, 0, StorageMetadata.State.LIVE, 1);
    assertThrows(IllegalStateException.class, () -> value.accept(StoredRecord.metadataOnly(key("b"), metadata)));
    assertThrows(IllegalStateException.class, value::finish);
    var wrongKey = new CompareEvaluator(CompareCondition.numeric(KeyRange.point(key("b")),
        Target.VERSION, Relation.EQUAL, 3));
    assertThrows(IllegalArgumentException.class, () -> wrongKey.accept(live("x", 3, 0, new byte[0])));
    assertThrows(IllegalStateException.class, wrongKey::finish);
    assertThrows(IllegalArgumentException.class, () -> CompareCondition.numeric(
        KeyRange.point(key("b")), Target.VALUE, Relation.EQUAL, 0));
  }

  @Test
  void unrecognizedNativeEnumValuesFollowThePinnedEtcdComparisonDefaults() {
    var unknownTarget = new CompareEvaluator(CompareCondition.numeric(KeyRange.point(key("b")),
        Target.UNRECOGNIZED, Relation.EQUAL, 99));
    unknownTarget.accept(live("b", 3, 0, new byte[] {1}));
    assertTrue(unknownTarget.finish());
    var unknownRelation = new CompareEvaluator(CompareCondition.numeric(KeyRange.point(key("b")),
        Target.VERSION, Relation.UNRECOGNIZED, Long.MAX_VALUE));
    unknownRelation.accept(live("b", 3, 0, new byte[] {1}));
    assertTrue(unknownRelation.finish());
    var absentValue = new CompareEvaluator(CompareCondition.value(KeyRange.point(key("b")),
        Relation.UNRECOGNIZED, new byte[0]));
    assertFalse(absentValue.finish());
  }

  private static boolean matches(Relation relation, int comparison) {
    return switch (relation) {
      case EQUAL -> comparison == 0;
      case NOT_EQUAL -> comparison != 0;
      case GREATER -> comparison > 0;
      case LESS -> comparison < 0;
      case UNRECOGNIZED -> true;
    };
  }

  private static StoredRecord live(String key, long version, long lease, byte[] value) {
    return StoredRecord.live(key(key), new StorageMetadata(2, 5, version, lease,
        StorageMetadata.State.LIVE, value.length), value);
  }

  private static StoredRecord tombstone(String key) {
    return StoredRecord.metadataOnly(key(key),
        new StorageMetadata(2, 5, 3, 0, StorageMetadata.State.TOMBSTONE, 0));
  }

  private static ByteKey key(String value) { return ByteKey.copyOf(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)); }

  private record NumericTarget(Target target, long number) {}
}
