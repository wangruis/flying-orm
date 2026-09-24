package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.internal.protection.ProtectedOwnerBatchPlan;
import com.flying.orm.rdb.result.DynamicRow;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcProtectedOwnerBatchPlanTest {

    @Test
    void rendersInternalSlotsAsPortableIntegersAndBindsOnlyOwnerValues() {
        List<ProtectedBatchRows.RowView> rows = List.of(
                row(updateWork("select id from business_row where id = ?", List.of(11L))),
                row(updateWork("select id from business_row where id = ?", List.of(22L))));

        ProtectedOwnerBatchPlan plan = ProtectedOwnerBatchPlan.plans(
                rows, Long.MAX_VALUE).iterator().next();

        assertEquals("select flying_owner_0.*, 0 as flying_orm_owner_slot"
                + " from (select id from business_row where id = ?) flying_owner_0"
                + " union all select flying_owner_1.*, 1 as flying_orm_owner_slot"
                + " from (select id from business_row where id = ?) flying_owner_1", plan.sql());
        assertEquals(List.of(11L, 22L), plan.parameters());
    }

    @Test
    void wideOwnerQueriesUseIndividualPlansAndStillHonorTheByteBudget() {
        int count = ProtectedOwnerBatchPlan.MAX_PARAMETERS + 1;
        ProtectedWriteWork work = updateWork("select id from business_row where id in ("
                + String.join(",", java.util.Collections.nCopies(count, "?")) + ")",
                java.util.Collections.nCopies(count, 1L));
        var rows = List.of(row(work), row(work));
        var plans = StreamSupport.stream(
                ProtectedOwnerBatchPlan.plans(rows, Long.MAX_VALUE).spliterator(), false).toList();
        assertEquals(List.of(1, 1), plans.stream().map(ProtectedOwnerBatchPlan::size).toList());
        assertEquals(count, plans.getFirst().parameters().size());
        assertThrows(IllegalArgumentException.class,
                () -> ProtectedOwnerBatchPlan.plans(rows, 1L).iterator().next());
    }

    @Test
    void reusesOwnerQueryParametersAlreadyOwnedByTheRequest() {
        AtomicInteger copies = new AtomicInteger();
        ProtectedWriteWork work = updateWork(
                "select id from business_row where seen_at = ?",
                List.of(new CountingDate(1L, copies)));
        copies.set(0);

        ProtectedOwnerBatchPlan.plans(
                List.of(row(work)), Long.MAX_VALUE).iterator().next();

        assertEquals(0, copies.get());
    }

    @Test
    void h2ExecutesDerivedUnionAndMapsEachOwnerBackToItsInputRow() throws Exception {
        try (Connection connection = connectionWithOwners()) {
            List<ProtectedBatchRows.RowView> rows = List.of(
                    row(updateWork("select id from business_row where id = ?", List.of(1L))),
                    row(updateWork("select id from business_row where id = ?", List.of(2L))));

            JdbcProtectedBatchSideIndex.Prepared prepared = new JdbcProtectedBatchSideIndex().prepare(
                    connection, request(), rows, null);

            assertEquals(Map.of("id", 1L), prepared.rows().get(0).owners().getFirst());
            assertEquals(Map.of("id", 2L), prepared.rows().get(1).owners().getFirst());
        }
    }

    @Test
    void rejectsTwoOwnerRowsForTheSameSlot() throws Exception {
        try (Connection connection = connectionWithOwners()) {
            List<ProtectedBatchRows.RowView> rows = List.of(row(updateWork(
                    "select id from business_row where id >= ?", List.of(1L))));

            SQLException error = assertThrows(SQLException.class, () ->
                    new JdbcProtectedBatchSideIndex().prepare(
                            connection, request(), rows, null));

            assertEquals("21000", error.getSQLState());
        }
    }

    @Test
    void splitsAtTheFixedRowBoundary() {
        List<ProtectedBatchRows.RowView> rows = new ArrayList<>();
        for (int index = 0; index <= ProtectedOwnerBatchPlan.MAX_ROWS; index++) {
            rows.add(row(updateWork(
                    "select id from business_row where id = ?", List.of((long) index))));
        }

        List<ProtectedOwnerBatchPlan> plans = StreamSupport.stream(
                ProtectedOwnerBatchPlan.plans(rows, Long.MAX_VALUE).spliterator(), false).toList();

        assertEquals(List.of(ProtectedOwnerBatchPlan.MAX_ROWS, 1),
                     plans.stream().map(ProtectedOwnerBatchPlan::size).toList());
        assertTrue(plans.getFirst().sql().contains(
                ", " + (ProtectedOwnerBatchPlan.MAX_ROWS - 1) + " as flying_orm_owner_slot"));
        assertTrue(plans.getLast().sql().contains(", 0 as flying_orm_owner_slot"));
        assertEquals(List.of((long) ProtectedOwnerBatchPlan.MAX_ROWS), plans.getLast().parameters());
    }

    @Test
    void splitsAtTheParameterAndByteBoundaries() {
        List<ProtectedBatchRows.RowView> parameterRows = new ArrayList<>();
        for (int index = 0; index < 401; index++) {
            parameterRows.add(row(updateWork(
                    "select id from business_row where id = ? or id = ? or id = ? or id = ? or id = ?",
                    List.of(1L, 2L, 3L, 4L, 5L))));
        }
        List<ProtectedOwnerBatchPlan> parameterPlans = StreamSupport.stream(
                ProtectedOwnerBatchPlan.plans(parameterRows, Long.MAX_VALUE).spliterator(), false).toList();
        List<ProtectedBatchRows.RowView> byteRows = List.of(
                row(updateWork("select id from business_row where id = ?", List.of(1L))),
                row(updateWork("select id from business_row where id = ?", List.of(2L))));
        List<ProtectedOwnerBatchPlan> bytePlans = StreamSupport.stream(
                ProtectedOwnerBatchPlan.plans(byteRows, 300L).spliterator(), false).toList();

        assertEquals(List.of(400, 1),
                     parameterPlans.stream().map(ProtectedOwnerBatchPlan::size).toList());
        assertEquals(List.of(1, 1),
                     bytePlans.stream().map(ProtectedOwnerBatchPlan::size).toList());
    }

    @Test
    void separatesDifferentOwnerQueryShapes() {
        List<ProtectedBatchRows.RowView> rows = List.of(
                row(updateWork("select id from business_row where id = ?", List.of(1L))),
                row(updateWork("select id from business_row where id >= ?", List.of(2L))));

        List<ProtectedOwnerBatchPlan> plans = StreamSupport.stream(
                ProtectedOwnerBatchPlan.plans(rows, Long.MAX_VALUE).spliterator(), false).toList();

        assertEquals(List.of(1, 1), plans.stream().map(ProtectedOwnerBatchPlan::size).toList());
        assertTrue(plans.stream().allMatch(plan -> plan.sql().contains("flying_orm_owner_slot")));
    }

    @Test
    void choosesASlotAliasThatCannotCollideWithAnOwnerColumn() {
        String ownerField = "flying_orm_owner_slot";
        ProtectedWriteWork work = updateWork(
                "select flying_orm_owner_slot from business_row where id = ?",
                List.of(1L), List.of(ownerField));
        ProtectedOwnerBatchPlan plan = ProtectedOwnerBatchPlan.plans(
                List.of(row(work)), Long.MAX_VALUE).iterator().next();
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        values.put(ownerField, 9L);
        values.put("flying_orm_owner_slot_", 0);

        ProtectedOwnerBatchPlan.Match match = plan.decode(DynamicRow.copyOf(values));

        assertTrue(plan.sql().contains("as flying_orm_owner_slot_ "));
        assertEquals(Map.of(ownerField, 9L), match.owner());
    }

    @Test
    void preservesNullOwnerQueryParametersInTheOwnedPlanContainer() {
        ProtectedWriteWork work = updateWork(
                "select id from business_row where value_col is not distinct from ?",
                java.util.Arrays.asList((Object) null));

        ProtectedOwnerBatchPlan plan = ProtectedOwnerBatchPlan.plans(
                List.of(row(work)), Long.MAX_VALUE).iterator().next();

        assertEquals(1, plan.parameters().size());
        assertEquals(null, plan.parameters().getFirst());
    }

    private static Connection connectionWithOwners() throws SQLException {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        Connection connection = dataSource.getConnection();
        try (java.sql.Statement statement = connection.createStatement()) {
            statement.execute("create table business_row(id bigint primary key, value_col varchar(40))");
            statement.execute("insert into business_row(id, value_col) values (1, 'a'), (2, 'b')");
        }
        return connection;
    }

    private static BatchWriteRequest request() {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "update business_row set value_col = ? where id = ?",
                2,
                List.of(String.class, Long.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.empty(),
                BatchWriteOptions.defaults());
    }

    private static ProtectedBatchRows.RowView row(ProtectedWriteWork work) {
        Object[] parameters = work.writeRequest().parameters().toArray();
        return ProtectedBatchRows.decode(
                ProtectedBatchRows.extend(parameters, work), parameters.length);
    }

    private static ProtectedWriteWork updateWork(String ownerSql, List<Object> ownerParameters) {
        return updateWork(ownerSql, ownerParameters, List.of("id"));
    }

    private static ProtectedWriteWork updateWork(String ownerSql,
                                                   List<Object> ownerParameters,
                                                   List<String> ownerFields) {
        return new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPDATE,
                new SqlRequest("update business_row set value_col = ? where id = ?",
                               List.of("value", 1L)),
                new SqlRequest(ownerSql, ownerParameters),
                ownerFields,
                Map.of(),
                "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone", List.of())));
    }

    private static final class CountingDate extends Date {
        private final AtomicInteger copies;

        private CountingDate(long time, AtomicInteger copies) {
            super(time);
            this.copies = copies;
        }

        @Override
        public Object clone() {
            copies.incrementAndGet();
            return new CountingDate(getTime(), copies);
        }
    }
}
