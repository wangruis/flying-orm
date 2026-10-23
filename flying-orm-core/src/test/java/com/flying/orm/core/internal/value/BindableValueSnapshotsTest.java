package com.flying.orm.core.internal.value;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.LogicDeleteDefinition;
import com.flying.orm.core.internal.condition.ConditionExecutionViews;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.CursorPosition;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.param.ParameterConditionSpec;
import com.flying.orm.core.scope.TenantScope;
import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class BindableValueSnapshotsTest {

    private static final int SCALAR_FAST_PATH_ITERATIONS = 10_000;

    @Test
    void ownedBufferAppendsSingleValuesWithoutATemporaryArray() {
        class IndexedValues extends ArrayList<Object> {
            private int arrayCopies;

            @Override
            public Object[] toArray() {
                arrayCopies++;
                return super.toArray();
            }
        }
        for (Object value : java.util.Arrays.asList("first", null, new byte[]{7})) {
            for (boolean published : List.of(false, true)) {
                IndexedValues source = new IndexedValues();
                source.add(value);
                OwnedBindableValues.Buffer buffer = OwnedBindableValues.buffer();
                buffer.add("prefix");
                buffer.addAll(published ? OwnedBindableValues.publishOwned(source) : source);
                List<Object> result = buffer.publish();
                assertEquals(java.util.Arrays.asList("prefix", value), result);
                assertEquals(0, source.arrayCopies, "appending must not materialize an intermediate parameter array");
                assertEquals(value instanceof byte[], OwnedBindableValues.requiresImmutableSnapshot(result));
                assertThrows(IllegalStateException.class, () -> buffer.addAll(List.of("late")));
                assertThrows(UnsupportedOperationException.class, () -> result.set(0, "changed"));
            }
        }
    }

    @Test
    void ownedBufferRetainsSequentialListTraversalAndMutableFlags() {
        class SequentialValues extends java.util.LinkedList<Object> {
            @Override
            public Object get(int index) {
                throw new AssertionError("sequential list must not be traversed by index");
            }
        }
        for (boolean published : List.of(false, true)) {
            SequentialValues source = new SequentialValues();
            byte[] bytes = {9};
            source.add(null);
            source.add(bytes);
            OwnedBindableValues.Buffer buffer = OwnedBindableValues.buffer();
            buffer.addAll(published ? OwnedBindableValues.publishOwned(source) : source);
            List<Object> result = buffer.publish();
            assertEquals(java.util.Arrays.asList(null, bytes), result);
            assertTrue(OwnedBindableValues.requiresImmutableSnapshot(result));
        }
    }

    @Test
    void publishedValuesRetainBulkArrayCopyWithoutIndexedTraversal() {
        class BulkValues extends ArrayList<Object> {
            private int indexedReads;

            @Override
            public Object get(int index) {
                indexedReads++;
                return super.get(index);
            }
        }
        BulkValues owned = new BulkValues();
        owned.add("first");
        owned.add(null);
        owned.add("last");
        List<Object> published = OwnedBindableValues.publishOwned(owned);

        Object[] copied = published.toArray();
        String[] typed = published.toArray(new String[0]);
        String[] oversized = {"old", "old", "old", "old", "untouched"};
        assertSame(oversized, published.toArray(oversized));
        assertEquals(0, owned.indexedReads, "owned bulk copy must not re-read every element by index");
        assertArrayEquals(new Object[]{"first", null, "last"}, copied);
        assertArrayEquals(new String[]{"first", null, "last"}, typed);
        assertArrayEquals(
                new String[]{"first", null, "last", null, "untouched"}, oversized);
        copied[0] = "changed";
        typed[2] = "changed";
        assertEquals("first", published.getFirst());
        assertEquals("last", published.getLast());
        assertThrows(ArrayStoreException.class, () -> published.toArray(new Integer[0]));
        assertThrows(UnsupportedOperationException.class, () -> published.set(0, "changed"));
        OwnedBindableValues.Buffer buffer = OwnedBindableValues.buffer(owned.size());
        int readsBeforeAppend = owned.indexedReads;
        buffer.addAll(published);
        assertEquals(readsBeforeAppend, owned.indexedReads,
                "bulk append must retain bulk copy rather than indexed traversal");
        assertEquals(owned, buffer.publish());
    }

    @Test
    void requestArrayCopiesStillIsolateMutableParameterValues() {
        byte[] bytes = {1, 2};
        OwnedBindableValues.Buffer buffer = OwnedBindableValues.buffer();
        buffer.add(bytes);
        SqlRequest request = new SqlRequest("select ?", buffer.publish());
        Object[] first = request.parameters().toArray();
        byte[][] second = request.parameters().toArray(new byte[0][]);
        ((byte[]) first[0])[0] = 9;
        second[0][1] = 8;
        byte[] retained = (byte[]) request.parameters().getFirst();
        assertArrayEquals(new byte[]{1, 2}, retained);
        assertArrayEquals(new byte[]{1, 2}, bytes);
    }

    @Test
    void ownedBufferTracksMutableValuesAfterRepeatedImmutableTypesAndBulkAdditions() {
        OwnedBindableValues.Buffer immutable = OwnedBindableValues.buffer();
        immutable.add(1);
        immutable.add(2);
        immutable.add(null);
        immutable.addAll(List.of(3L, 4L, 5));
        List<Object> immutableValues = immutable.publish();
        assertFalse(OwnedBindableValues.requiresImmutableSnapshot(immutableValues));
        assertEquals(java.util.Arrays.asList(1, 2, null, 3L, 4L, 5), immutableValues);

        for (boolean bulk : List.of(false, true)) {
            OwnedBindableValues.Buffer buffer = OwnedBindableValues.buffer();
            buffer.addAll(immutableValues);
            buffer.truncate(1);
            buffer.add(6);
            byte[] bytes = {7};
            if (bulk) buffer.addAll(List.of(bytes, 8));
            else buffer.add(bytes);
            List<Object> values = buffer.publish();
            assertTrue(OwnedBindableValues.requiresImmutableSnapshot(values));
            List<Object> exposed = OwnedBindableValues.defensiveView(values);
            assertNotSame(bytes, exposed.get(2));
            assertEquals((byte) 7, ((byte[]) exposed.get(2))[0]);
            assertThrows(IllegalStateException.class, () -> buffer.add(9));
        }
    }

    @Test
    void logicalValuesRemainReadOnlyAndIsolateBorrowedLists() {
        List<Object> published = OwnedBindableValues.publishOwned(new ArrayList<>(List.of("name", 1, 2L)));
        List<Object> publishedSnapshot = BindableValueSnapshots.logicalValues(published);
        assertEquals(published, publishedSnapshot);
        assertThrows(UnsupportedOperationException.class, () -> publishedSnapshot.add("changed"));

        List<Object> borrowed = new ArrayList<>(List.of("name", 1));
        List<Object> snapshot = BindableValueSnapshots.logicalValues(borrowed);
        borrowed.set(0, "changed");
        assertEquals("name", snapshot.getFirst());
    }

    @Test
    void publishedMutableLogicalValuesStillReceiveIndependentSnapshots() {
        StringBuilder text = new StringBuilder("value");
        Timestamp time = Timestamp.from(Instant.parse("2026-01-01T00:00:00Z"));
        List<Object> published = OwnedBindableValues.publishOwned(new ArrayList<>(List.of(text, time)));
        List<Object> first = BindableValueSnapshots.logicalValues(published);
        List<Object> second = BindableValueSnapshots.logicalValues(published);
        assertNotSame(first, second);
        assertInstanceOf(StringBuilder.class, first.getFirst()).append("changed");
        ((Timestamp) first.get(1)).setTime(0);
        assertEquals("value", second.getFirst().toString());
        assertEquals(time, second.get(1));
    }

    @Test
    void classifiesActualMutableTypesWithoutTreatingAllNumbersAsImmutable() {
        List<Object> mutable = List.of(new byte[]{1}, new Object[]{1},
                ByteBuffer.wrap(new byte[]{1}), ByteBuffer.wrap(new byte[]{1}).asReadOnlyBuffer(),
                new StringBuilder("text"), new StringBuffer("text"), CharBuffer.wrap("text"),
                new java.util.Date(1), new java.sql.Date(1), new java.sql.Time(1), new Timestamp(1),
                new TextNumber());
        for (Object value : mutable) {
            assertTrue(BindableValueSnapshots.requiresImmutableSnapshot(value), value.getClass().getName());
        }
        for (Object value : List.of("text", 42, 42L, true, LocalDate.of(2026, 10, 5),
                TestEnum.VALUE, List.of(1), Map.of("key", "value"), new Object())) {
            assertFalse(BindableValueSnapshots.requiresImmutableSnapshot(value), value.getClass().getName());
            assertSame(value, BindableValueSnapshots.logicalScalar(value));
            assertSame(value, BindableValueSnapshots.immutableScalar(value, null));
        }
        assertFalse(BindableValueSnapshots.requiresImmutableSnapshot(null));
        TextNumber text = new TextNumber();
        assertSame(text, BindableValueSnapshots.logicalValue(text));
        assertEquals("42", BindableValueSnapshots.immutableValue(text));
    }

    @Test
    void preservesLogicalAndBindingPoliciesForDateTextHybrid() {
        TextDate value = new TextDate();
        assertTrue(BindableValueSnapshots.requiresImmutableSnapshot(value));
        assertSame(value, BindableValueSnapshots.logicalScalar(value));
        assertEquals("date", BindableValueSnapshots.immutableScalar(value, null));
        assertSame(value, BindableValueSnapshots.arrayGraph(value));
    }

    @TestFactory
    List<DynamicTest> sharedTextKeepsScalarAndTypedArrayRepresentationsSeparate() {
        List<DynamicTest> tests = new ArrayList<>();
        for (Class<?> type : List.of(StringBuilder.class, StringBuffer.class, CharBuffer.class)) {
            for (boolean scalarFirst : List.of(true, false)) {
                tests.add(DynamicTest.dynamicTest(type.getSimpleName() + " scalarFirst=" + scalarFirst, () -> {
                    CharSequence source = type == StringBuilder.class ? new StringBuilder("text")
                            : type == StringBuffer.class ? new StringBuffer("text")
                            : CharBuffer.wrap(new char[]{'t', 'e', 'x', 't'});
                    Object array = Array.newInstance(type, 2);
                    Array.set(array, 0, source);
                    Array.set(array, 1, source);
                    List<Object> values = scalarFirst ? List.of(source, array) : List.of(array, source);
                    List<Object> snapshot = BindableValueSnapshots.immutableValues(values);
                    SqlRequest request = new SqlRequest("select ?, ?", values);
                    if (source instanceof StringBuilder text) text.setCharAt(0, 'X');
                    else if (source instanceof StringBuffer text) text.setCharAt(0, 'X');
                    else ((CharBuffer) source).put(0, 'X');
                    for (List<Object> copied : List.of(snapshot, request.parameters())) {
                        assertEquals("text", copied.get(scalarFirst ? 0 : 1));
                        Object typed = copied.get(scalarFirst ? 1 : 0);
                        assertEquals(type, typed.getClass().getComponentType());
                        assertEquals("text", Array.get(typed, 0).toString());
                        assertSame(Array.get(typed, 0), Array.get(typed, 1));
                        assertNotSame(source, Array.get(typed, 0));
                    }
                }));
            }
        }
        return tests;
    }

    @Test
    void mixedStandardTextRangePreservesOriginalValuesAndBinding() {
        ConditionGroup range = ConditionGroup.and().where("label", "between",
                List.of(new StringBuilder(" a "), "z")).build();
        assertEquals(List.of(" a ", "z"),
                     ConditionExecutionViews.bindParameters(range, ValueCodecRegistry.standard()));
        ConditionGroup reversed = ConditionGroup.and().where("label", "between",
                List.of(new StringBuilder("z"), new StringBuffer("a"))).build();
        assertEquals(List.of("z", "a"),
                ConditionExecutionViews.bindParameters(reversed, ValueCodecRegistry.standard()));
    }

    @Test
    void logicalTextKeepsItsTypeAcrossConditionAndScopeBoundaries() {
        StringBuilder source = new StringBuilder(" value ");
        ConditionGroup condition = ConditionGroup.and().where("label", "=", source).build();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> type) {
                return type == StringBuilder.class;
            }

            @Override
            public Object write(Object value) {
                return "codec:" + ((StringBuilder) value);
            }

            @Override
            public Object read(Object value, Class<?> type) {
                return new StringBuilder(value.toString());
            }
        });
        source.setCharAt(1, 'X');
        assertEquals(List.of("codec: value "), ConditionExecutionViews.bindParameters(condition, codecs));

        List<Object> published = List.of(
                TenantScope.of("tenant", new StringBuilder(" value ")).value(),
                ParameterConditionSpec.builder("p", "label", "=").defaultValue(source).build().defaultValue(),
                CursorPosition.of(List.of(source)).values().getFirst(),
                KeysetPageQuery.after(10, com.flying.orm.core.page.CursorPosition.of(List.of(source)), KeysetSort.asc("label", com.flying.orm.core.page.NullOrder.LAST)).position().values().getFirst(),
                LogicDeleteDefinition.of("deleted", source, new StringBuilder("yes")).notDeletedValue());
        published.forEach(value -> assertInstanceOf(StringBuilder.class, value));
    }

    @Test
    void logicalTextArraysRemainTypedWhileBindingSnapshotsKeepTheirOldContract() {
        StringBuilder source = new StringBuilder("text");
        StringBuilder[] array = {source, source};
        StringBuilder[] copied = (StringBuilder[]) BindableValueSnapshots.logicalValue(array);
        source.setCharAt(0, 'X');
        assertSame(copied[0], copied[1]);
        assertEquals("text", copied[0].toString());
        assertEquals("Xext", BindableValueSnapshots.immutableValue(source));
        CharBuffer chars = (CharBuffer) BindableValueSnapshots.logicalValue(CharBuffer.wrap(" chars "));
        assertEquals(" chars ", ValueCodecRegistry.standard().write(chars));
        assertEquals(" chars ", BindableValueSnapshots.immutableValue(chars));
    }

    @Test
    void copiesMutableScalarsOnceAcrossSharedRoots() {
        ByteBuffer source = ByteBuffer.wrap(new byte[]{1, 2});

        List<Object> snapshot = BindableValueSnapshots.immutableValues(List.of(source, source));

        assertSame(snapshot.get(0), snapshot.get(1));
        assertTrue(((ByteBuffer) snapshot.getFirst()).isReadOnly());
    }

    @TestFactory
    List<DynamicTest> mixedScalarSequencesPreserveSharedMutableCopies() {
        List<DynamicTest> tests = new ArrayList<>();
        for (boolean logical : List.of(true, false)) {
            tests.add(DynamicTest.dynamicTest("logical=" + logical, () -> {
                Timestamp time = Timestamp.from(Instant.parse("2026-10-05T00:00:00.123456789Z"));
                byte[] bytes = {1, 2};
                Object domainValue = new Object();
                List<Object> values = new ArrayList<>();
                values.add("stable");
                values.add(42);
                values.add(null);
                values.add(domainValue);
                values.add(time);
                values.add(bytes);
                values.add(new Timestamp[]{time});
                values.add(bytes);
                List<Object> snapshot = logical ? BindableValueSnapshots.logicalValues(values)
                        : BindableValueSnapshots.immutableValues(values);

                assertEquals(values.subList(0, 4), snapshot.subList(0, 4));
                assertSame(domainValue, snapshot.get(3));
                assertNotSame(time, snapshot.get(4));
                assertSame(snapshot.get(4), ((Timestamp[]) snapshot.get(6))[0]);
                assertNotSame(bytes, snapshot.get(5));
                assertSame(snapshot.get(5), snapshot.get(7));
                time.setTime(0);
                bytes[0] = 9;
                values.clear();
                assertEquals(Instant.parse("2026-10-05T00:00:00.123456789Z"),
                        ((Timestamp) snapshot.get(4)).toInstant());
                assertEquals(1, ((byte[]) snapshot.get(5))[0]);
                assertThrows(UnsupportedOperationException.class, () -> snapshot.add("changed"));
            }));
        }
        return tests;
    }

    @Test
    void preservesSharedArrayIdentityAcrossOneOwnedValue() {
        byte[] child = {1};
        Object[] source = {child, child};

        Object[] snapshot = (Object[]) BindableValueSnapshots.immutableValue(source);

        assertNotSame(source, snapshot);
        assertSame(snapshot[0], snapshot[1]);
    }

    @Test
    void rejectsCyclicObjectArraysInsteadOfOwningAnArbitraryGraph() {
        Object[] source = new Object[1];
        source[0] = source;

        assertThrows(IllegalArgumentException.class,
                     () -> BindableValueSnapshots.immutableValue(source));
    }

    @Test
    void retainsTimestampNanoseconds() {
        Timestamp source = Timestamp.from(Instant.parse("2026-08-24T01:02:03.123456789Z"));

        Timestamp snapshot = (Timestamp) BindableValueSnapshots.immutableValue(source);

        assertNotSame(source, snapshot);
        assertEquals(source, snapshot);
        assertEquals(source.getNanos(), snapshot.getNanos());
    }

    @Test
    void arrayOnlyPolicyDoesNotClaimApplicationScalars() {
        ByteBuffer source = ByteBuffer.wrap(new byte[]{1});

        assertSame(source, BindableValueSnapshots.arrayGraph(source));
    }

    @Test
    void doesNotTraverseRawContainersOrUnknownDriverValues() {
        Map<String, Object> map = new LinkedHashMap<>();
        List<Object> list = new ArrayList<>();
        Object driverValue = new Object();

        assertSame(map, BindableValueSnapshots.immutableValue(map));
        assertSame(list, BindableValueSnapshots.immutableValue(list));
        assertSame(driverValue, BindableValueSnapshots.immutableValue(driverValue));
    }

    @Test
    void immutableScalarFastPathDoesNotAllocateSnapshotSessions() {
        java.lang.management.ThreadMXBean managementBean = ManagementFactory.getThreadMXBean();
        assumeTrue(managementBean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) managementBean;
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        Object[] scalars = {
                "stable", 42, true, TestEnum.VALUE,
                UUID.fromString("50c8f56e-cdae-4e60-93c3-72160481b435"),
                LocalDate.of(2026, 8, 29)
        };
        for (int iteration = 0; iteration < SCALAR_FAST_PATH_ITERATIONS; iteration++) {
            BindableValueSnapshots.immutableValue(scalars[iteration % scalars.length]);
        }

        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        Object result = null;
        for (int iteration = 0; iteration < SCALAR_FAST_PATH_ITERATIONS; iteration++) {
            result = BindableValueSnapshots.immutableValue(scalars[iteration % scalars.length]);
        }
        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;

        assertSame(scalars[(SCALAR_FAST_PATH_ITERATIONS - 1) % scalars.length], result);
        assertTrue(allocated < 1_024,
                   () -> "immutable scalar snapshots allocated session state: " + allocated + " bytes");
    }

    private enum TestEnum {
        VALUE
    }

    private static final class TextNumber extends Number implements CharSequence {
        @Override public int intValue() { return 42; }
        @Override public long longValue() { return 42; }
        @Override public float floatValue() { return 42; }
        @Override public double doubleValue() { return 42; }
        @Override public int length() { return 2; }
        @Override public char charAt(int index) { return "42".charAt(index); }
        @Override public CharSequence subSequence(int start, int end) { return "42".subSequence(start, end); }
        @Override public String toString() { return "42"; }
    }

    private static final class TextDate extends java.util.Date implements CharSequence {
        @Override public int length() { return 4; }
        @Override public char charAt(int index) { return "date".charAt(index); }
        @Override public CharSequence subSequence(int start, int end) { return "date".subSequence(start, end); }
        @Override public String toString() { return "date"; }
    }
}
