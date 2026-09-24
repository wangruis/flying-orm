package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StructuredConditionDeepSnapshotTest {

    private static final int DEPTH = 10_000;

    @Test
    void deepListsReachTheConfiguredDepthCheckWithoutUsingTheThreadStack() {
        assertDeepSnapshot(Shape.LIST);
    }

    @Test
    void deepMapsReachTheConfiguredDepthCheckWithoutUsingTheThreadStack() {
        assertDeepSnapshot(Shape.MAP);
    }

    @Test
    void deepArraysReachTheConfiguredDepthCheckWithoutUsingTheThreadStack() {
        assertDeepSnapshot(Shape.ARRAY);
    }

    @Test
    void aSetContainingADeepListCanBeSnapshottedWithoutRecursiveHashing() {
        Object source = nested(7, Shape.LIST);
        StructuredConditionInput input = StructuredConditionInput.term(
                "payload", "raw", Collections.singleton(source));

        Set<?> snapshot = assertInstanceOf(Set.class, input.value());
        assertEquals(1, snapshot.size());
        assertEquals(7, leaf(snapshot.iterator().next(), Shape.LIST));
        assertNotSame(source, snapshot.iterator().next());
        assertThrows(UnsupportedOperationException.class, snapshot::clear);
    }

    @Test
    void aTwoElementSetKeepsDeepAndOrdinaryMembers() {
        Object source = nested(7, Shape.LIST);
        StructuredConditionInput input = StructuredConditionInput.term(
                "payload", "raw", Set.of(source, "ordinary"));

        Set<?> snapshot = assertInstanceOf(Set.class, input.value());
        assertEquals(2, snapshot.size());
        assertTrue(snapshot.contains("ordinary"));
        assertTrue(snapshot.contains(source));
    }

    @Test
    void deepContainerHashCollisionsDoNotMergeUnequalMembers() {
        Set<Object> source = Collections.newSetFromMap(new IdentityHashMap<>());
        source.add(mixedNested(new StringBuilder("Aa")));
        source.add(mixedNested(new StringBuilder("BB")));
        StructuredConditionInput input = StructuredConditionInput.term("payload", "raw", source);

        Set<?> snapshot = assertInstanceOf(Set.class, input.value());
        assertEquals(2, snapshot.size());
        assertTrue(snapshot.contains(mixedNested("Aa")));
        assertTrue(snapshot.contains(mixedNested("BB")));
    }

    @Test
    void deepMembersThatBecomeEqualAfterNormalizationStillFail() {
        Set<Object> source = Collections.newSetFromMap(new IdentityHashMap<>());
        source.add(mixedNested(new StringBuilder("same")));
        source.add(mixedNested(new StringBuilder("same")));

        StructuredConditionException failure = assertThrows(StructuredConditionException.class,
                () -> StructuredConditionInput.term("payload", "raw", source));
        assertEquals(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED, failure.code());
    }

    @Test
    void deepSetsKeepStructuralEqualityAndHashingAcrossIndependentReads() {
        Object source = 7;
        for (int level = 0; level < DEPTH; level++) source = Collections.singleton(source);
        StructuredConditionInput input = StructuredConditionInput.term("payload", "raw", source);

        Set<?> first = assertInstanceOf(Set.class, input.value());
        Set<?> second = assertInstanceOf(Set.class, input.value());
        assertNotSame(first, second);
        assertEquals(7, first.hashCode());
        assertEquals(first, second);
    }

    @Test
    void setSnapshotsKeepNullOrderAndIdentitySemanticsForArrayAndGeneralCollectionMembers() {
        int[] array = {7};
        Collection<?> collection = Collections.unmodifiableCollection(List.of("value"));
        Set<Object> source = new LinkedHashSet<>();
        source.add(null);
        source.add(array);
        source.add(collection);
        source.add("last");
        StructuredConditionInput input = StructuredConditionInput.term("payload", "raw", source);

        Set<?> snapshot = assertInstanceOf(Set.class, input.value());
        List<?> members = new ArrayList<>(snapshot);
        assertNull(members.get(0));
        int[] copiedArray = assertInstanceOf(int[].class, members.get(1));
        Collection<?> copiedCollection = assertInstanceOf(Collection.class, members.get(2));
        assertEquals("last", members.get(3));
        assertTrue(snapshot.contains(null));
        assertTrue(snapshot.contains(copiedArray));
        assertTrue(snapshot.contains(copiedCollection));
        assertFalse(snapshot.contains(array));
        assertFalse(snapshot.contains(collection));
        copiedArray[0] = 9;
        List<?> later = new ArrayList<>(assertInstanceOf(Set.class, input.value()));
        assertEquals(7, assertInstanceOf(int[].class, later.get(1))[0]);
    }

    @Test
    void setHashReflectsMutationOfAnIndependentlyPublishedScalarCopy() {
        StructuredConditionInput input = StructuredConditionInput.term("payload", "raw", Set.of(new Date(7)));
        Set<?> snapshot = assertInstanceOf(Set.class, input.value());
        Date copy = assertInstanceOf(Date.class, snapshot.iterator().next());
        copy.setTime(19);

        assertEquals(copy.hashCode(), snapshot.hashCode());
        Set<?> later = assertInstanceOf(Set.class, input.value());
        assertEquals(7, assertInstanceOf(Date.class, later.iterator().next()).getTime());
    }

    @Test
    void deepSharedValuesPreserveAliasesAndRemainIsolatedAcrossReads() {
        byte[] leaf = {7};
        Object nested = nested(leaf, Shape.LIST);
        StructuredConditionInput input = StructuredConditionInput.term("payload", "raw", List.of(nested, nested));
        leaf[0] = 9;

        List<?> first = assertInstanceOf(List.class, input.value());
        assertSame(first.get(0), first.get(1));
        byte[] firstLeaf = assertInstanceOf(byte[].class, leaf(first.getFirst(), Shape.LIST));
        assertEquals(7, firstLeaf[0]);
        firstLeaf[0] = 11;

        List<?> second = assertInstanceOf(List.class, input.value());
        assertSame(second.get(0), second.get(1));
        assertNotSame(first.getFirst(), second.getFirst());
        assertEquals(7, assertInstanceOf(byte[].class, leaf(second.getFirst(), Shape.LIST))[0]);
    }

    @Test
    void aCycleBelowADeepContainerPathStillProducesTheShapeError() {
        List<Object> cycle = new ArrayList<>();
        cycle.add(cycle);
        Object nested = nested(cycle, Shape.LIST);

        StructuredConditionException failure = assertThrows(StructuredConditionException.class,
                () -> StructuredConditionInput.term("payload", "raw", nested));
        assertEquals(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED, failure.code());
        assertEquals("conditions.value", failure.path());
    }

    private static void assertDeepSnapshot(Shape shape) {
        Object source = nested(7, shape);
        StructuredConditionInput input = StructuredConditionInput.term("payload", "raw", source);
        Object snapshot = input.value();
        assertNotSame(source, snapshot);
        assertEquals(7, leaf(snapshot, shape));

        StructuredConditionException failure = assertThrows(StructuredConditionException.class,
                () -> StructuredConditionCompiler.validateStructure(input,
                        StructuredConditionPolicy.defaults().allowOperator("raw")));
        assertEquals(StructuredConditionErrorCode.DEPTH_EXCEEDED, failure.code());
    }

    private static Object nested(Object value, Shape shape) {
        for (int level = 0; level < DEPTH; level++) {
            value = switch (shape) {
                case LIST -> List.of(value);
                case MAP -> Map.of("child", value);
                case ARRAY -> new Object[]{value};
            };
        }
        return value;
    }

    private static Object mixedNested(Object value) {
        for (int level = 0; level < DEPTH; level++) {
            value = switch (level % 3) {
                case 0 -> List.of(value);
                case 1 -> Map.of("child", value);
                default -> Collections.singleton(value);
            };
        }
        return value;
    }

    private static Object leaf(Object value, Shape shape) {
        for (int level = 0; level < DEPTH; level++) {
            value = switch (shape) {
                case LIST -> ((List<?>) value).getFirst();
                case MAP -> ((Map<?, ?>) value).get("child");
                case ARRAY -> ((Object[]) value)[0];
            };
        }
        return value;
    }

    private enum Shape { LIST, MAP, ARRAY }
}
