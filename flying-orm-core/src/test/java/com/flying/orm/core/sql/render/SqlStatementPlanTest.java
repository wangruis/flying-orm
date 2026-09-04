package com.flying.orm.core.sql.render;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlStatementPlanTest {

    private static final int IMMUTABLE_REQUEST_ITERATIONS = 10_000;

    private static final long MAX_IMMUTABLE_REQUEST_ALLOCATION = 256L;

    @Test
    void keepsReusableSqlStructureSeparateFromRequestValues() {
        SqlStatementPlan plan = SqlStatementPlan.prepared(
                "select ?", SqlBindMarkerStyle.CANONICAL, 1,
                "POSTGRESQL", "select $1");
        List<Object> parameters = List.of(7);

        SqlRequest request = new SqlRequest(plan, parameters);

        assertEquals("select ?", request.sql());
        assertEquals("select $1", request.statement().transportSql("POSTGRESQL").orElseThrow());
        assertSame(parameters, request.parameters());
    }

    @Test
    void rejectsValuesThatDoNotMatchTheCompiledParameterLayout() {
        SqlStatementPlan plan = SqlStatementPlan.prepared(
                "select ?, ?", SqlBindMarkerStyle.CANONICAL, 2,
                "POSTGRESQL", "select $1, $2");

        assertThrows(IllegalArgumentException.class, () -> new SqlRequest(plan, List.of(1)));
    }

    @Test
    void snapshotsOneMutableReferenceOnlyOncePerRequest() {
        byte[] source = {1, 2};
        byte[] encodedSource = {3, 4};

        SqlRequest request = new SqlRequest("select ?, ?", List.of(source, source));
        SqlRequest encodedRequest = new SqlRequest(
                "select ?, ?", List.of(SqlFragment.encodedParameter(encodedSource),
                                        SqlFragment.encodedParameter(encodedSource)));
        source[0] = 9;
        encodedSource[0] = 9;

        assertEquals(1, ((byte[]) request.parameters().get(0))[0]);
        assertSame(request.parameters().get(0), request.parameters().get(1));
        assertEquals(3, ((byte[]) encodedRequest.parameters().get(0))[0]);
        assertSame(encodedRequest.parameters().get(0), encodedRequest.parameters().get(1));
    }

    @Test
    void unwrapsNullableImmutableParametersIntoAnImmutableRequestList() {
        List<Object> source = new ArrayList<>();
        source.add(null);
        source.add(SqlFragment.encodedParameter("stable"));
        source.add(42);

        SqlRequest request = new SqlRequest("select ?, ?, ?", source);
        source.set(2, 99);

        assertNull(request.parameters().get(0));
        assertEquals("stable", request.parameters().get(1));
        assertEquals(42, request.parameters().get(2));
        assertThrows(UnsupportedOperationException.class,
                     () -> request.parameters().add("unexpected"));
    }

    @Test
    void nullableAndEncodedImmutableParametersAvoidMutableSnapshotSessionAllocation() {
        java.lang.management.ThreadMXBean managementBean = ManagementFactory.getThreadMXBean();
        assumeTrue(managementBean instanceof com.sun.management.ThreadMXBean,
                   "per-thread allocation counters are unavailable");
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) managementBean;
        assumeTrue(allocationBean.isThreadAllocatedMemorySupported(),
                   "per-thread allocation counters are unsupported");
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        assumeTrue(allocationBean.isThreadAllocatedMemoryEnabled(),
                   "per-thread allocation counters could not be enabled");
        SqlStatementPlan plan = SqlStatementPlan.canonical(
                "select ?, ?, ?", SqlBindMarkerStyle.CANONICAL, 3);
        List<Object> parameters = new ArrayList<>();
        parameters.add(null);
        parameters.add(SqlFragment.encodedParameter("stable"));
        parameters.add(42);
        for (int iteration = 0; iteration < 1_000; iteration++) {
            new SqlRequest(plan, parameters);
        }

        SqlRequest[] requests = new SqlRequest[IMMUTABLE_REQUEST_ITERATIONS];
        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        for (int iteration = 0; iteration < IMMUTABLE_REQUEST_ITERATIONS; iteration++) {
            requests[iteration] = new SqlRequest(plan, parameters);
        }
        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;
        long perRequest = allocated / IMMUTABLE_REQUEST_ITERATIONS;

        SqlRequest last = requests[IMMUTABLE_REQUEST_ITERATIONS - 1];
        assertNull(last.parameters().get(0));
        assertEquals("stable", last.parameters().get(1));
        assertEquals(42, last.parameters().get(2));
        assertTrue(perRequest < MAX_IMMUTABLE_REQUEST_ALLOCATION,
                   () -> "immutable request parameters allocated snapshot-session state: "
                           + perRequest + " bytes per request");
    }

    @Test
    void reportsNoTransportSqlBeforeDialectCompilation() {
        SqlStatementPlan plan = SqlStatementPlan.canonical(
                "select ?", SqlBindMarkerStyle.CANONICAL, 1);

        assertTrue(plan.transportSql("").isEmpty());
    }

    @Test
    void preparedFactoryNormalizesTheTransportDialect() {
        SqlStatementPlan plan = SqlStatementPlan.prepared(
                "select ?", SqlBindMarkerStyle.CANONICAL, 1,
                " postgresql ", "select $1");

        assertTrue(plan.preparedFor("POSTGRESQL"));
        assertEquals("select $1", plan.transportSql("postgresql").orElseThrow());
    }
}
