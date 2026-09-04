package com.flying.orm.core.internal.value;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class BindableValueSnapshotsTest {

    private static final int SCALAR_FAST_PATH_ITERATIONS = 10_000;

    @Test
    void copiesMutableScalarsOnceAcrossSharedRoots() {
        ByteBuffer source = ByteBuffer.wrap(new byte[]{1, 2});

        List<Object> snapshot = BindableValueSnapshots.immutableValues(List.of(source, source));

        assertSame(snapshot.get(0), snapshot.get(1));
        assertTrue(((ByteBuffer) snapshot.getFirst()).isReadOnly());
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
}
