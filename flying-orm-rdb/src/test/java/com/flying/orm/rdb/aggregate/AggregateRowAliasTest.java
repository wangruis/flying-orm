package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.internal.value.OwnedBindableValues;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AggregateRowAliasTest {
    private static volatile AggregateRowLayout layoutSink;

    @Test
    void layoutOwnsOneIndexWithoutRebuildingItForPublication() {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                        .noneMatch(argument -> argument.startsWith("-javaagent:")),
                "allocation limits require an uninstrumented VM");
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean)
                java.lang.management.ManagementFactory.getThreadMXBean();
        allocation.setThreadAllocatedMemoryEnabled(true);
        List<GroupSelection> groups = List.of(GroupSelection.of("status", "status"));
        List<AggregateExpression<?>> aggregates = List.of(AggregateExpression.count("id", "row_count"),
                AggregateExpression.count("score", "nonnull_count"), AggregateExpression.sum("score", "total_score"));
        int iterations = 50_000;
        for (int index = 0; index < iterations; index++) layoutSink = AggregateRowLayout.of(groups, aggregates);
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        for (int index = 0; index < iterations; index++) layoutSink = AggregateRowLayout.of(groups, aggregates);
        long bytes = allocation.getThreadAllocatedBytes(thread) - before;
        assertTrue(bytes < iterations * 512L, "bytes per aggregate layout: " + bytes / (double) iterations);
        assertEquals(0, layoutSink.indexOf(groups.getFirst()));
        assertEquals(3, layoutSink.indexOf(aggregates.getLast()));
        List<GroupSelection> input = new ArrayList<>(groups);
        AggregateRowLayout isolated = AggregateRowLayout.of(input, aggregates);
        input.clear();
        assertEquals(groups, isolated.groups());
        assertThrows(UnsupportedOperationException.class, () -> isolated.groups().clear());
    }

    @Test
    void oneIndexOwnsTypedAndAliasLookupWithoutMergingTheirIdentities() {
        assertEquals(1, Arrays.stream(AggregateRowLayout.class.getDeclaredFields())
                .filter(field -> Map.class.isAssignableFrom(field.getType())).count());
        GroupSelection group = GroupSelection.of("department", "same");
        AggregateExpression<Long> count = AggregateExpression.countRows("same");
        AggregateRow row = AggregateRow.of(AggregateRowLayout.of(List.of(group), List.of(count)),
                List.of("sales", 2L));
        assertEquals("sales", row.get(GroupSelection.of("department", "same"), String.class));
        assertEquals(2L, row.get(AggregateExpression.countRows("same")));
        assertThrows(IllegalArgumentException.class, () -> row.get("same", Object.class));
        assertThrows(IllegalArgumentException.class, () -> AggregateRowLayout.of(List.of(group, group), List.of()));
        assertThrows(IllegalArgumentException.class, () -> AggregateRowLayout.of(List.of(), List.of(count, count)));
        assertEquals(0, AggregateRowLayout.of(List.of(), List.of()).size());
    }

    @Test
    void immutableOwnedValuesAreRetainedWithoutAnotherListCopy() throws ReflectiveOperationException {
        List<Object> owned = OwnedBindableValues.publishOwned(new ArrayList<>(Arrays.asList("sales", null, 7L)));
        AggregateRowLayout layout = AggregateRowLayout.of(
                List.of(GroupSelection.of("department", "department"), GroupSelection.of("missing", "missing")),
                List.of(AggregateExpression.countRows("total")));
        AggregateRow row = AggregateRow.of(layout, owned);
        Field storage = AggregateRow.class.getDeclaredField("values");
        storage.setAccessible(true);
        assertSame(owned, storage.get(row));
        assertEquals(Arrays.asList("sales", null, 7L), row.values());
        assertThrows(UnsupportedOperationException.class, () -> row.values().set(0, "changed"));
    }

    @Test
    void ownedMutableValuesStillRequireSnapshotsAndPreserveSharedPayloads() {
        byte[] bytes = {1, 2};
        Date date = new Date(123L);
        StringBuilder text = new StringBuilder("before");
        List<Object> owned = OwnedBindableValues.publishOwned(new ArrayList<>(List.of(bytes, bytes, date, text)));
        AggregateRow row = AggregateRow.of(AggregateRowLayout.of(List.of(
                GroupSelection.of("a", "a"), GroupSelection.of("b", "b"),
                GroupSelection.of("date", "date"), GroupSelection.of("text", "text")), List.of()), owned);
        bytes[0] = 9;
        date.setTime(456L);
        text.append("changed");
        List<Object> exported = row.values();
        assertArrayEquals(new byte[]{1, 2}, (byte[]) exported.get(0));
        assertSame(exported.get(0), exported.get(1));
        assertEquals(123L, ((Date) exported.get(2)).getTime());
        assertEquals("before", exported.get(3).toString());
        ((byte[]) exported.get(0))[0] = 8;
        assertArrayEquals(new byte[]{1, 2}, row.get("a", byte[].class));
    }

    @Test
    void ordinaryCallerListsAreAlwaysCopied() {
        List<Object> values = new ArrayList<>(List.of(7L));
        AggregateRow row = AggregateRow.of(AggregateRowLayout.of(List.of(),
                List.of(AggregateExpression.countRows("total"))), values);
        values.set(0, 9L);
        assertEquals(7L, row.get("total", Long.class));
    }

    @Test
    void readsGroupAndAggregateByTheirDeclaredAlias() {
        GroupSelection group = GroupSelection.of("department", "Department");
        AggregateExpression<Long> count = AggregateExpression.countRows("total");
        AggregateRowLayout layout = AggregateRowLayout.of(List.of(group), List.of(count));
        AggregateRow row = AggregateRow.of(layout, List.of("sales", 7L));

        assertEquals(row.get(group, String.class), row.get("Department", String.class));
        assertEquals(row.get(count), row.get("total", Long.class));
        assertEquals(7L, row.get("total", Number.class));
        AggregateRow other = AggregateRow.of(layout, List.of("support", 2L));
        assertSame(row.layout(), other.layout());
        assertEquals(2L, other.get("total", Long.class));
    }

    @Test
    void nullRemainsNullButMissingAliasAndWrongTypeAreNotSilentlyAccepted() {
        AggregateRow row = AggregateRow.of(AggregateRowLayout.of(List.of(),
                List.of(AggregateExpression.sum("amount", "sum"), AggregateExpression.countRows("total"))),
                Arrays.asList(null, 4L));

        assertNull(row.get("sum", BigDecimal.class));
        assertThrows(IllegalArgumentException.class, () -> row.get("unknown", Long.class));
        assertThrows(IllegalArgumentException.class, () -> row.get("TOTAL", Long.class));
        assertThrows(ClassCastException.class, () -> row.get("total", String.class));
    }

    @Test
    void aliasReadPreservesTheExistingMutablePayloadIsolation() {
        byte[] original = {1, 2};
        GroupSelection group = GroupSelection.of("payload", "bytes");
        AggregateRow row = AggregateRow.of(AggregateRowLayout.of(List.of(group),
                List.of(AggregateExpression.countRows("total"))), List.of(original, 1L));
        original[0] = 9;
        byte[] read = row.get("bytes", byte[].class);
        assertArrayEquals(new byte[]{1, 2}, read);
        read[0] = 8;
        assertArrayEquals(new byte[]{1, 2}, row.get("bytes", byte[].class));
        assertArrayEquals(new byte[]{1, 2}, row.get(group, byte[].class));
    }

    @Test
    void ambiguousManualLayoutsRetainTypedAccessButCannotSilentlyChooseAnAlias() {
        GroupSelection group = GroupSelection.of("department", "total");
        AggregateExpression<Long> count = AggregateExpression.countRows("total");
        AggregateRow row = AggregateRow.of(AggregateRowLayout.of(List.of(group), List.of(count)),
                List.of("sales", 7L));

        assertEquals("sales", row.get(group, String.class));
        assertEquals(7L, row.get(count));
        assertThrows(IllegalArgumentException.class, () -> row.get("total", Object.class));
    }
}
