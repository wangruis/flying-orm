package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Snapshot contracts preserved while moving budget checks ahead of collection storage allocation. */
class StructuredConditionValueSnapshotsBudgetTest {

    private static final int MAX_REFERENCES = 20_000;

    @Test
    void listChargesItsReferencesOnceAtTheBoundary() {
        assertCollectionBoundary(size -> Collections.nCopies(size, null));
    }

    @Test
    void generalCollectionChargesItsReferencesOnceAtTheBoundary() {
        assertCollectionBoundary(size -> Collections.unmodifiableCollection(Collections.nCopies(size, null)));
    }

    @Test
    void setRetainsItsReferenceBoundary() {
        assertCollectionBoundary(size -> {
            Collection<Integer> values = new LinkedHashSet<>();
            for (int index = 0; index < size; index++) values.add(index);
            return values;
        });
    }

    @Test
    void siblingCollectionsShareTheRemainingReferenceBudget() {
        Collection<?> first = Collections.unmodifiableCollection(Collections.nCopies(10_000, null));
        List<?> atLimit = List.of(first, Collections.nCopies(9_998, null));
        List<?> aboveLimit = List.of(first, Collections.nCopies(9_999, null));

        assertEquals(2, assertInstanceOf(List.class, input(atLimit).value()).size());
        assertFailure(aboveLimit, StructuredConditionErrorCode.NODE_COUNT_EXCEEDED);
    }

    @Test
    void mapKeysAndValuesReduceTheBudgetAvailableToNestedCollections() {
        Collection<?> first = Collections.unmodifiableCollection(Collections.nCopies(10_000, null));
        Map<?, ?> atLimit = Map.of("first", first, "second", Collections.nCopies(9_996, null));
        Map<?, ?> aboveLimit = Map.of("first", first, "second", Collections.nCopies(9_997, null));

        assertEquals(2, assertInstanceOf(Map.class, input(atLimit).value()).size());
        assertFailure(aboveLimit, StructuredConditionErrorCode.NODE_COUNT_EXCEEDED);
    }

    @Test
    void excessiveCollectionDepthPrecedesTheReferenceFailure() {
        List<?> oversized = Collections.nCopies(MAX_REFERENCES + 1, null);
        assertFailure(atDepth(oversized, 65), StructuredConditionErrorCode.DEPTH_EXCEEDED);
        assertFailure(atDepth(Collections.unmodifiableCollection(oversized), 65),
                      StructuredConditionErrorCode.DEPTH_EXCEEDED);
    }

    @Test
    void sharedCollectionIsChargedOnceAndEachPublicReadKeepsItsOwnAliases() {
        List<Object> source = new ArrayList<>(Collections.nCopies(MAX_REFERENCES - 2, null));
        Collection<Object> shared = Collections.unmodifiableCollection(source);
        StructuredConditionInput input = input(List.of(shared, shared));
        source.set(0, "changed after publication");

        List<?> first = assertInstanceOf(List.class, input.value());
        Collection<?> child = assertInstanceOf(Collection.class, first.getFirst());
        assertSame(first.getFirst(), first.get(1));
        assertNotSame(shared, child);
        assertNull(child.iterator().next());
        assertThrows(UnsupportedOperationException.class, child::clear);

        List<?> second = assertInstanceOf(List.class, input.value());
        assertSame(second.getFirst(), second.get(1));
        assertNotSame(first.getFirst(), second.getFirst());
        assertNull(assertInstanceOf(Collection.class, second.getFirst()).iterator().next());
    }

    @Test
    void listAndGeneralCollectionCyclesKeepTheShapeFailure() {
        List<Object> list = new ArrayList<>();
        list.add(list);
        assertFailure(list, StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED);

        List<Object> members = new ArrayList<>();
        Collection<Object> collection = Collections.unmodifiableCollection(members);
        members.add(collection);
        assertFailure(collection, StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED);
    }

    private static void assertCollectionBoundary(IntFunction<? extends Collection<?>> values) {
        Collection<?> snapshot = assertInstanceOf(Collection.class, input(values.apply(MAX_REFERENCES)).value());
        assertEquals(MAX_REFERENCES, snapshot.size());
        assertThrows(UnsupportedOperationException.class, snapshot::clear);
        assertFailure(values.apply(MAX_REFERENCES + 1), StructuredConditionErrorCode.NODE_COUNT_EXCEEDED);
    }

    private static StructuredConditionInput input(Object value) {
        return StructuredConditionInput.term("payload", "json-contains", value);
    }

    private static Object atDepth(Object leaf, int depth) {
        Object value = leaf;
        for (int level = 0; level < depth; level++) value = List.of(value);
        return value;
    }

    private static void assertFailure(Object value, StructuredConditionErrorCode code) {
        StructuredConditionException failure = assertThrows(StructuredConditionException.class, () -> input(value));
        assertEquals(code, failure.code());
        assertEquals("conditions.value", failure.path());
    }
}
