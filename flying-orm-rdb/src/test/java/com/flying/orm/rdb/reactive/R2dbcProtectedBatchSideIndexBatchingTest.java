package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.internal.protection.ProtectedReplacementBatchPlan;
import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class R2dbcProtectedBatchSideIndexBatchingTest {

    private static final int ROW_COUNT = 8;

    @TestFactory
    List<DynamicTest> singleRowUpsertUsesCountsWithoutReturningInputSlots() {
        return List.of(List.of(List.<Integer>of()), List.of(List.of(0))).stream()
                .map(groups -> DynamicTest.dynamicTest(groups.toString(),
                        () -> verifyUpsertSlots(groups, false, false, 1))).toList();
    }

    @TestFactory
    List<DynamicTest> nativeUpsertUsesReturnedInputSlotsInsteadOfResultOrderOrOwnerKeys() {
        return List.of(List.of(List.<Integer>of()), List.of(List.of(0)), List.of(List.of(1)),
                List.of(List.of(1, 0)), List.of(List.<Integer>of(), List.of(1)),
                List.of(List.of(-1)), List.of(List.of(2)), List.of(List.of(1, 1)))
                .stream().map(groups -> DynamicTest.dynamicTest(groups.toString(),
                        () -> verifyUpsertSlots(groups, false, false))).toList();
    }

    @Test
    void returnedSlotsBeforeAnErrorDoNotPublishCompletionOrWriteTokens() {
        verifyUpsertSlots(List.of(List.of(0)), true, false);
    }

    @Test
    void returnedSlotsBeforeCancellationDoNotPublishCompletionOrWriteTokens() {
        verifyUpsertSlots(List.of(List.of(0)), false, true);
    }

    private static void verifyUpsertSlots(List<List<Integer>> groups, boolean fail, boolean cancel) {
        verifyUpsertSlots(groups, fail, cancel, 2);
    }

    private static void verifyUpsertSlots(List<List<Integer>> groups, boolean fail, boolean cancel, int rowCount) {
        StatementRecorder recorder = new StatementRecorder();
        recorder.businessSlots = groups;
        recorder.businessFailure = fail ? new IllegalStateException("business result failed") : null;
        recorder.holdBusinessResults = cancel;
        com.flying.orm.core.form.DynamicForm form = com.flying.orm.core.form.DynamicForm
                .builder("business_row", "business_row")
                .addField(com.flying.orm.core.form.DynamicField.of("content", "VARCHAR"))
                .addField(com.flying.orm.core.form.DynamicField.primaryKey("id", "BIGINT")).build();
        BatchWriteRequest rendered = com.flying.orm.rdb.form.FormDataSqlRenderer.create(
                com.flying.orm.core.sql.render.SqlRenderer.builder().addDefaultTerms().build(),
                RdbDialect.postgresql()).upsertBatch(form,
                List.<Map<String, Object>>of(Map.of("content", "first", "id", 7L),
                        Map.of("content", "second", "id", 7L)).subList(0, rowCount),
                BatchWriteOptions.of(2));
        List<Object[]> parameters = Flux.from(rendered.rows()).collectList().block();
        List<Object[]> protectedRows = new ArrayList<>();
        for (int index = 0; index < parameters.size(); index++) {
            ProtectedWriteWork work = new ProtectedWriteWork(ProtectedWriteWork.Kind.UPSERT,
                    new SqlRequest(rendered.sql(), List.of(parameters.get(index))), null,
                    List.of("id"), Map.of("id", 7L), "id = ?",
                    "delete from token_index where id = ? and field_tag = ?",
                    "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                    List.of(tokens("phone", index)));
            protectedRows.add(ProtectedBatchRows.extend(parameters.get(index), work));
        }
        BatchWriteRequest request = new BatchWriteRequest(rendered.statement(), rendered.parameterTypes(),
                Flux.fromIterable(protectedRows), rendered.options(), rendered.rowCountPolicy(), rendered.generatedKeys());
        List<Long> completed = new ArrayList<>();
        Mono<com.flying.orm.rdb.batch.BatchExecutionEvidence> write = R2dbcSqlExecutor.create(
                ConnectionAccessTestSupport.borrowed(recorder.connection()), RdbDialect.postgresql())
                .writeProtectedBatch(request, offset -> Mono.fromRunnable(() -> completed.add(offset)));
        List<Integer> slots = groups.stream().flatMap(List::stream).toList();
        if (cancel) {
            List<Object> signals = new ArrayList<>();
            reactor.core.Disposable subscription = write.subscribe(signals::add, signals::add,
                    () -> signals.add("complete"));
            try {
                assertEquals(1, recorder.businessExecutions);
            } finally {
                subscription.dispose();
            }
            assertEquals(List.of(), signals);
            assertEquals(1, recorder.executions.size());
            assertEquals(List.of(), completed);
            return;
        }
        if (fail || slots.stream().anyMatch(index -> index < 0 || index >= 2)
                || slots.stream().distinct().count() != slots.size()) {
            assertThrows(com.flying.orm.rdb.batch.BatchExecutionEvidenceException.class,
                    () -> write.block(Duration.ofSeconds(5)));
            assertEquals(1, recorder.executions.size(), "invalid outcomes must not write the side index");
            assertEquals(List.of(), completed);
            return;
        }
        com.flying.orm.rdb.batch.BatchExecutionEvidence evidence = write.block(Duration.ofSeconds(5));
        List<Integer> written = groups.stream().flatMap(List::stream).sorted().toList();
        assertEquals(written.stream().map(index -> List.of(7L, "phone", token(index))).toList(),
                recorder.executions.stream().filter(execution -> execution.sql().startsWith("insert into token_index"))
                        .flatMap(execution -> execution.parameters().stream()).toList());
        assertEquals(written.size(), evidence.affectedRows().value());
        assertEquals(java.util.stream.LongStream.range(0, rowCount).boxed().toList(), completed);
        assertEquals(1, recorder.businessExecutions, "keep the parameter batch as one execution");
        Execution main = recorder.executions.getFirst();
        if (rowCount == 1) {
            assertTrue(!main.sql().contains("returning"));
            assertEquals(parameters.stream().map(row -> List.of(row)).toList(), main.parameters());
        } else {
            assertTrue(main.sql().endsWith(" returning cast($3 as integer)"));
            assertEquals(List.of(0, 1), main.parameters().stream().map(List::getLast).toList());
        }
        assertTrue(!rendered.sql().contains("returning"), "JDBC and ordinary writes keep their SQL");
    }

    @TestFactory
    List<DynamicTest> lobWriteCountsExcludeOnlyConfirmedZeroRows() {
        List<DynamicTest> tests = new ArrayList<>();
        for (ProtectedWriteWork.Kind kind : List.of(ProtectedWriteWork.Kind.INSERT, ProtectedWriteWork.Kind.UPSERT)) {
            for (List<List<Long>> counts : List.of(
                    List.of(List.of(1L), List.of(0L)), List.of(List.of(0L), List.of(1L)),
                    List.of(List.of(-2L), List.of(0L)), List.of(List.of(0L), List.<Long>of()),
                    List.of(List.<Long>of(), List.of(0L)), List.of(List.of(0L, 1L), List.of(0L)),
                    List.of(List.of(-2L, 0L), List.of(0L)))) {
                tests.add(DynamicTest.dynamicTest(kind + "/" + counts, () -> verifyLobCounts(kind, counts)));
            }
        }
        return tests;
    }

    private static void verifyLobCounts(ProtectedWriteWork.Kind kind, List<List<Long>> counts) {
        StatementRecorder recorder = new StatementRecorder();
        recorder.businessCounts = counts;
        String sql = "insert into business_row(content, id) values (?, ?)";
        List<Object[]> rows = new ArrayList<>();
        List<List<Object>> expectedTokens = new ArrayList<>();
        for (int index = 0; index < counts.size(); index++) {
            long id = index + 1;
            Object[] values = {new SqlTypedValue(SqlTypedValue.Kind.BLOB, new byte[]{(byte) id}), id};
            ProtectedWriteWork work = new ProtectedWriteWork(kind, new SqlRequest(sql, List.of(values)),
                    null, List.of("id"), Map.of("id", id), "id = ?",
                    "delete from token_index where id = ? and field_tag = ?",
                    "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                    List.of(tokens("phone", (int) id)));
            rows.add(ProtectedBatchRows.extend(values, work));
            if (!counts.get(index).equals(List.of(0L))) expectedTokens.add(List.of(id, "phone", token((int) id)));
        }
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(sql, 2,
                List.of(byte[].class, Long.class), SqlBindMarkerStyle.CANONICAL,
                Flux.fromIterable(rows), BatchWriteOptions.of(2));
        List<Long> completed = new ArrayList<>();
        com.flying.orm.rdb.batch.BatchExecutionEvidence evidence = R2dbcSqlExecutor.create(
                ConnectionAccessTestSupport.borrowed(recorder.connection()), RdbDialect.postgresql())
                .writeProtectedBatch(request, offset -> Mono.fromRunnable(() -> completed.add(offset)))
                .block(Duration.ofSeconds(5));
        assertEquals(expectedTokens, recorder.executions.stream()
                .filter(execution -> execution.sql().startsWith("insert into token_index"))
                .flatMap(execution -> execution.parameters().stream()).toList());
        assertEquals(kind == ProtectedWriteWork.Kind.UPSERT
                        ? expectedTokens.stream().map(row -> row.subList(0, 2)).toList() : List.of(),
                recorder.executions.stream().filter(execution -> execution.sql().startsWith("delete from token_index"))
                        .flatMap(execution -> execution.parameters().stream()).toList());
        assertEquals(2, recorder.businessExecutions);
        assertEquals(List.of(0L, 1L), completed);
        boolean known = counts.stream().allMatch(row -> !row.isEmpty() && row.stream().allMatch(count -> count >= 0));
        assertEquals(known, evidence.affectedRows().isKnown());
        if (known) assertEquals(1L, evidence.affectedRows().value());
    }

    @Test
    void compositeOwnerTokenBatchesRespectParameterAndOperationLimits() {
        StatementRecorder recorder = new StatementRecorder();
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into business_row(a, b, c) values (?, ?, ?)", List.of(7L, 8L, 9L)),
                null, List.of("a", "b", "c"), Map.of("a", 7L, "b", 8L, "c", 9L),
                "a = ? and b = ? and c = ?",
                "delete from token_index where a = ? and b = ? and c = ? and field_tag = ?",
                "insert into token_index(a, b, c, field_tag, token) values (?, ?, ?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone",
                        java.util.stream.IntStream.range(0, 501).mapToObj(index -> token(index).array()).toList())));

        new R2dbcProtectedBatchSideIndex(R2dbcBindMarkers.from(RdbDialect.postgresql()))
                .complete(recorder.connection(), new ProtectedReplacementBatchPlan.Prepared(List.of(
                        new ProtectedReplacementBatchPlan.Row(work, List.of())),
                        com.flying.orm.rdb.batch.BatchWriteOptions.DEFAULT_MAX_BUFFERED_BYTES))
                .block(Duration.ofSeconds(5));

        assertTrue(recorder.executions.stream().allMatch(execution ->
                execution.parameters().size() <= 500
                        && execution.parameters().stream().mapToInt(List::size).sum() <= 2_000));
        assertEquals(java.util.stream.IntStream.range(0, 501)
                        .mapToObj(index -> List.of(7L, 8L, 9L, "phone", token(index))).toList(),
                recorder.executions.stream().flatMap(execution -> execution.parameters().stream()).toList());
    }


    @Test
    void ordinaryChunkDoesNotPreparePerRowProtectionState() {
        List<ProtectedBatchRows.RowView> rows = new ArrayList<>();
        for (int index = 0; index < 500; index++) {
            rows.add(ProtectedBatchRows.admit(new Object[]{"plain", (long) index}, 2, Long.MAX_VALUE, "test bytes"));
        }
        R2dbcBatchWriterChunks.BatchChunk chunk = new R2dbcBatchWriterChunks.BatchChunk(0L, rows);
        ProtectedReplacementBatchPlan.Prepared prepared = new R2dbcProtectedBatchSideIndex(
                R2dbcBindMarkers.from(RdbDialect.postgresql())).prepare(null, request(), chunk,
                        () -> { throw new AssertionError("plain chunk must not acquire LOB state"); })
                .block(Duration.ofSeconds(5));

        assertEquals(List.of(), prepared.rows());
    }

    @Test
    void ownerPreReadsUseOneBoundedStatementForTheChunk() {
        StatementRecorder recorder = new StatementRecorder();
        Connection connection = recorder.connection();
        List<ProtectedBatchRows.RowView> rows = new ArrayList<>();
        for (int index = 0; index < ROW_COUNT; index++) {
            rows.add(row(updateWork(1_000L + index)));
        }
        R2dbcBatchWriterChunks.BatchChunk chunk = new R2dbcBatchWriterChunks.BatchChunk(
                0, rows);

        new R2dbcProtectedBatchSideIndex(R2dbcBindMarkers.from(RdbDialect.postgresql()))
                .prepare(connection, request(), chunk, R2dbcLargeObjectScope::new)
                .block(Duration.ofSeconds(5));

        assertEquals(1, recorder.statements.get(),
                     "owner pre-read Statements must be bounded by the chunk, not multiplied by rows");
    }

    @Test
    void sideIndexDeletesUseOneBoundedStatementForTheChunk() {
        StatementRecorder recorder = new StatementRecorder();
        Connection connection = recorder.connection();
        List<ProtectedReplacementBatchPlan.Row> states = new ArrayList<>();
        for (int index = 0; index < ROW_COUNT; index++) {
            states.add(new ProtectedReplacementBatchPlan.Row(
                    upsertWork(1_000L + index), List.of()));
        }

        new R2dbcProtectedBatchSideIndex(R2dbcBindMarkers.from(RdbDialect.postgresql()))
                .complete(connection, new ProtectedReplacementBatchPlan.Prepared(states,
                        com.flying.orm.rdb.batch.BatchWriteOptions.DEFAULT_MAX_BUFFERED_BYTES))
                .block(Duration.ofSeconds(5));

        assertEquals(1, recorder.statements.get(),
                     "side-index DELETE Statements must be bounded by the chunk, not multiplied by owner fields");
    }

    @Test
    void updateReplacementBatchesNonEmptyTokensAcrossOwners() {
        assertReplacementBatchesAcrossOwners(ProtectedWriteWork.Kind.UPDATE);
    }

    @Test
    void upsertReplacementBatchesNonEmptyTokensAcrossOwners() {
        assertReplacementBatchesAcrossOwners(ProtectedWriteWork.Kind.UPSERT);
    }

    private static void assertReplacementBatchesAcrossOwners(ProtectedWriteWork.Kind kind) {
        StatementRecorder recorder = new StatementRecorder();
        List<ProtectedReplacementBatchPlan.Row> states = new ArrayList<>();
        for (int index = 0; index < 500; index++) {
            states.add(state(kind, 1_000L + index, List.of(tokens("phone", index))));
        }

        complete(recorder, states);

        assertEquals(List.of("DELETE:500", "INSERT:500"), recorder.executionShapes(),
                     "replacement token execute calls must be batched across owners");
        Execution insert = recorder.executions.get(1);
        for (int index = 0; index < 500; index++) {
            assertEquals(List.of(1_000L + index, "phone", token(index)), insert.parameters().get(index));
        }
    }

    @Test
    void replacementTokensAcrossOwnersAndFieldsUseBoundedBatches() {
        StatementRecorder recorder = new StatementRecorder();
        List<byte[]> fieldTokens = java.util.stream.IntStream.range(0, 260)
                .mapToObj(index -> token(index).array()).toList();
        List<ProtectedWriteWork.FieldTokens> fields = List.of(
                new ProtectedWriteWork.FieldTokens("phone", fieldTokens),
                new ProtectedWriteWork.FieldTokens("email", fieldTokens));

        complete(recorder, List.of(
                state(ProtectedWriteWork.Kind.UPSERT, 7L, fields),
                state(ProtectedWriteWork.Kind.UPSERT, 8L, fields)));

        assertEquals(List.of("DELETE:4", "INSERT:500", "INSERT:500", "INSERT:40"),
                     recorder.executionShapes());
        List<List<Object>> parameters = recorder.executions.stream().skip(1)
                .flatMap(execution -> execution.parameters().stream()).toList();
        int offset = 0;
        for (long owner : List.of(7L, 8L)) {
            for (String field : List.of("phone", "email")) {
                for (int index = 0; index < 260; index++) {
                    assertEquals(List.of(owner, field, token(index)), parameters.get(offset++));
                }
            }
        }
    }

    @Test
    void repeatedOwnerFieldCompletesItsInsertBeforeTheNextDelete() {
        StatementRecorder recorder = new StatementRecorder();

        complete(recorder, List.of(
                state(ProtectedWriteWork.Kind.UPSERT, 7L, List.of(tokens("phone", 1))),
                state(ProtectedWriteWork.Kind.UPSERT, 8L, List.of(tokens("phone", 2))),
                state(ProtectedWriteWork.Kind.UPSERT, 7L, List.of(tokens("phone", 3)))));

        assertEquals(List.of("DELETE:2", "INSERT:2", "DELETE:1", "INSERT:1"),
                     recorder.executionShapes());
        assertEquals(List.of(List.of(7L, "phone", token(1)), List.of(8L, "phone", token(2))),
                     recorder.executions.get(1).parameters());
        assertEquals(List.of(List.of(7L, "phone", token(3))),
                     recorder.executions.get(3).parameters());
    }

    @Test
    void incorrectReplacementInsertCountStopsBeforeTheNextSegment() {
        StatementRecorder recorder = new StatementRecorder();
        recorder.insertCountAdjustment = -1L;
        List<ProtectedReplacementBatchPlan.Row> states = List.of(
                state(ProtectedWriteWork.Kind.UPSERT, 7L, List.of(tokens("phone", 1))),
                state(ProtectedWriteWork.Kind.UPSERT, 8L, List.of(tokens("phone", 2))),
                state(ProtectedWriteWork.Kind.UPSERT, 7L, List.of(tokens("phone", 3))));

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> complete(recorder, states));

        assertEquals("protected side index insert batch must affect one row per token", error.getMessage());
        assertEquals(List.of("DELETE:2", "INSERT:2"), recorder.executionShapes());
    }

    private static void complete(StatementRecorder recorder,
                                 List<ProtectedReplacementBatchPlan.Row> states) {
        Connection connection = recorder.connection();
        new R2dbcProtectedBatchSideIndex(R2dbcBindMarkers.from(RdbDialect.postgresql()))
                .complete(connection, new ProtectedReplacementBatchPlan.Prepared(states,
                        com.flying.orm.rdb.batch.BatchWriteOptions.DEFAULT_MAX_BUFFERED_BYTES))
                .block(Duration.ofSeconds(5));
    }

    @Test
    void decimalOwnerScaleDoesNotReorderReplacementStatements() {
        StatementRecorder recorder = new StatementRecorder();
        complete(recorder, List.of(
                state(ProtectedWriteWork.Kind.UPSERT, new BigDecimal("1.0"), List.of(tokens("phone", 1))),
                state(ProtectedWriteWork.Kind.UPSERT, new BigDecimal("1.00"), List.of(tokens("phone", 1)))));

        assertEquals(List.of("DELETE:1", "INSERT:1", "DELETE:1", "INSERT:1"),
                     recorder.executionShapes());
        assertEquals(new BigDecimal("1.0"), recorder.executions.get(1).parameters().getFirst().getFirst());
        assertEquals(new BigDecimal("1.00"), recorder.executions.get(3).parameters().getFirst().getFirst());
    }

    @TestFactory
    List<DynamicTest> numericOwnerIdentityControlsReplacementSegmentation() {
        // Floating-point equality is limited to exact integers/zero, not database rounding rules.
        record Owners(String name, Number first, Number second, boolean same) { }
        return List.of(
                new Owners("Integer-Long", 7, 7L, true),
                new Owners("BigInteger-Long", BigInteger.valueOf(7), 7L, true),
                new Owners("BigDecimal-Long", new BigDecimal("7.00"), 7L, true),
                new Owners("Float-Double-exact-integer", 7.0F, 7.0D, true),
                new Owners("negative-double-zero-Long", -0.0D, 0L, true),
                new Owners("negative-float-zero-Double", -0.0F, 0.0D, true),
                new Owners("different-integral-values", 7, 8L, false),
                new Owners("different-decimal-values", new BigDecimal("7.01"), 7L, false),
                new Owners("different-longs-above-double-precision", 9_007_199_254_740_992L,
                        9_007_199_254_740_993L, false),
                new Owners("opposite-infinities", Float.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, false),
                new Owners("NaN-and-finite", Double.NaN, 7L, false))
                .stream().map(owners -> DynamicTest.dynamicTest(owners.name(), () -> {
                    StatementRecorder recorder = new StatementRecorder();
                    complete(recorder, List.of(
                            state(ProtectedWriteWork.Kind.UPSERT, owners.first(), List.of(tokens("phone", 1))),
                            state(ProtectedWriteWork.Kind.UPSERT, owners.second(), List.of(tokens("phone", 2)))));

                    assertEquals(owners.same()
                                    ? List.of("DELETE:1", "INSERT:1", "DELETE:1", "INSERT:1")
                                    : List.of("DELETE:2", "INSERT:2"), recorder.executionShapes());
                    List<List<Object>> inserts = new ArrayList<>();
                    List<List<Object>> deletes = new ArrayList<>();
                    for (int index = 0; index < recorder.executions.size(); index++) {
                        (recorder.executionShapes().get(index).startsWith("INSERT:") ? inserts : deletes)
                                .addAll(recorder.executions.get(index).parameters());
                    }
                    assertEquals(List.of(List.of(owners.first(), "phone"), List.of(owners.second(), "phone")),
                            deletes, "identity normalization must not rewrite owner bindings");
                    assertEquals(List.of(List.of(owners.first(), "phone", token(1)),
                            List.of(owners.second(), "phone", token(2))), inserts);
                })).toList();
    }

    private static ProtectedReplacementBatchPlan.Row state(ProtectedWriteWork.Kind kind,
                                                               Object ownerId,
                                                               List<ProtectedWriteWork.FieldTokens> fields) {
        boolean update = kind == ProtectedWriteWork.Kind.UPDATE;
        ProtectedWriteWork work = work(kind, ownerId,
                update ? new SqlRequest("select id from business_row where id = ?", List.of(ownerId)) : null,
                fields);
        return new ProtectedReplacementBatchPlan.Row(
                work, update ? List.of(Map.of("id", ownerId)) : List.of());
    }

    private static ProtectedWriteWork.FieldTokens tokens(String field, int value) {
        return new ProtectedWriteWork.FieldTokens(field, List.of(token(value).array()));
    }

    private static ByteBuffer token(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(0, value);
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
        return ProtectedBatchRows.admit(
                ProtectedBatchRows.extend(parameters, work), parameters.length, Long.MAX_VALUE, "test bytes");
    }

    private static ProtectedWriteWork updateWork(long ownerId) {
        return work(
                ProtectedWriteWork.Kind.UPDATE,
                ownerId,
                new SqlRequest("select id from business_row where id = ?", List.of(ownerId)));
    }

    private static ProtectedWriteWork upsertWork(long ownerId) {
        return work(ProtectedWriteWork.Kind.UPSERT, ownerId, null);
    }

    private static ProtectedWriteWork work(ProtectedWriteWork.Kind kind,
                                             long ownerId,
                                             SqlRequest ownerQuery) {
        return work(kind, ownerId, ownerQuery,
                    List.of(new ProtectedWriteWork.FieldTokens("phone", List.of())));
    }

    private static ProtectedWriteWork work(ProtectedWriteWork.Kind kind,
                                             Object ownerId,
                                             SqlRequest ownerQuery,
                                             List<ProtectedWriteWork.FieldTokens> fields) {
        return new ProtectedWriteWork(
                kind,
                new SqlRequest("update business_row set value_col = ? where id = ?",
                               List.of("value", ownerId)),
                ownerQuery,
                List.of("id"),
                kind == ProtectedWriteWork.Kind.UPDATE ? Map.of() : Map.of("id", ownerId),
                "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                fields);
    }

    private record Execution(String sql, List<List<Object>> parameters) {
    }

    private static final class StatementRecorder {

        private final AtomicInteger statements = new AtomicInteger();
        private final List<Execution> executions = new ArrayList<>();
        private long insertCountAdjustment;
        private List<List<Long>> businessCounts;
        private List<List<Integer>> businessSlots;
        private RuntimeException businessFailure;
        private boolean holdBusinessResults;
        private int businessExecutions;

        private List<String> executionShapes() {
            return executions.stream().map(execution ->
                    (execution.sql().startsWith("delete") ? "DELETE:" : "INSERT:")
                            + execution.parameters().size()).toList();
        }

        private Connection connection() {
            return (Connection) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{Connection.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "createStatement" -> statement((String) arguments[0]);
                        case "toString" -> "protected-batch-counting-connection";
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

        private Statement statement(String sql) {
            statements.incrementAndGet();
            Map<Integer, Object> bindings = new TreeMap<>();
            List<List<Object>> parameterSets = new ArrayList<>();
            Statement[] statement = new Statement[1];
            statement[0] = (Statement) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{Statement.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "bind" -> {
                            bindings.put((Integer) arguments[0], arguments[1]);
                            yield statement[0];
                        }
                        case "bindNull" -> throw new AssertionError("side-index parameters must not be null");
                        case "add" -> {
                            parameterSets.add(List.copyOf(bindings.values()));
                            bindings.clear();
                            yield statement[0];
                        }
                        case "execute" -> {
                            parameterSets.add(List.copyOf(bindings.values()));
                            executions.add(new Execution(sql, List.copyOf(parameterSets)));
                            if (businessSlots != null && sql.startsWith("insert into \"business_row\"")) {
                                businessExecutions++;
                                Flux<Result> results = Flux.fromIterable(businessSlots).map(this::slotResult);
                                yield businessFailure != null ? results.concatWith(Mono.error(businessFailure))
                                        : holdBusinessResults ? results.concatWith(Flux.never()) : results;
                            }
                            if (sql.startsWith("insert into business_row")) {
                                yield Mono.just(result(businessCounts.get(businessExecutions++)));
                            }
                            long count = parameterSets.size()
                                    + (sql.startsWith("insert") ? insertCountAdjustment : 0L);
                            yield Mono.just(result(count));
                        }
                        case "toString" -> "protected-batch-counting-statement";
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
            return statement[0];
        }

        private Result result(long count) {
            return result(List.of(count));
        }

        private Result slotResult(List<Integer> slots) {
            return (Result) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Result.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "getRowsUpdated" -> Mono.just((long) slots.size());
                        case "map" -> Flux.fromIterable(slots).map(slot -> {
                            io.r2dbc.spi.Row row = (io.r2dbc.spi.Row) Proxy.newProxyInstance(getClass().getClassLoader(),
                                    new Class<?>[]{io.r2dbc.spi.Row.class}, (ignored, accessor, args) -> {
                                        if (accessor.getName().equals("get")) return slot;
                                        throw new UnsupportedOperationException(accessor.getName());
                                    });
                            @SuppressWarnings("unchecked")
                            java.util.function.BiFunction<io.r2dbc.spi.Row, io.r2dbc.spi.RowMetadata, Object> mapper =
                                    (java.util.function.BiFunction<io.r2dbc.spi.Row, io.r2dbc.spi.RowMetadata, Object>) arguments[0];
                            return mapper.apply(row, null);
                        });
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

        private Result result(List<Long> counts) {
            return (Result) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{Result.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "map" -> Flux.empty();
                        case "getRowsUpdated" -> Flux.fromIterable(counts);
                        case "toString" -> "protected-batch-counting-result";
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }
    }
}
