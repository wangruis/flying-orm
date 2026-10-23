package com.flying.orm.rdb.execution;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.BatchUpdate;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtectedBatchSequentialUpdateTest {

    @BeforeAll
    static void initializeRealEncryptionEntropy() {
        // 首次 JDK 取种子不属于批量计数/取消契约的五秒等待；仍使用真实安全随机源。
        new java.security.SecureRandom().nextBytes(new byte[32]);
    }

    @TestFactory
    List<DynamicTest> mergeIgnoreMapsOnlyInsertedRowsToSearchTokens() {
        List<DynamicTest> tests = new ArrayList<>();
        for (RdbDialect dialect : List.of(RdbDialect.oracle(), RdbDialect.sqlServer(), RdbDialect.h2())) {
            for (boolean reactive : List.of(false, true)) {
                tests.add(DynamicTest.dynamicTest(dialect.name() + "/reactive=" + reactive, () -> {
                    DynamicForm form = DynamicForm.builder("sequential", "sequential")
                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                            .addField(DynamicField.of("secret", "VARCHAR"))
                            .encrypted("secret", EncryptedFieldDefinition.builder()
                                    .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
                    try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                            ProtectedFieldKeyRing.single("v1", new byte[32]))) {
                        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                                SqlRenderer.builder().addDefaultTerms().build(), dialect).withProtectedFields(runtime);
                        Database database = new Database();
                        database.insertRowCounts = List.of(0L, 1L);
                        database.aggregateInsertCounts = true;
                        database.tokens.addAll(tokens(runtime, form, "alphabet"));
                        BatchSpec spec = BatchSpec.upsert(form, Flux.just(
                                Map.<String, Object>of("id", 1L, "secret", "ignored"),
                                Map.<String, Object>of("id", 2L, "secret", "inserted")))
                                .ignoreConflicts().withOptions(BatchWriteOptions.of(2));
                        BatchExecutionEvidence evidence;
                        if (reactive) {
                            evidence = ReactiveFormClient.create(
                                    R2dbcSqlExecutor.create(database.reactiveAccess(), dialect), renderer)
                                    .writeBatch(spec).block(Duration.ofSeconds(5));
                        } else {
                            com.flying.orm.rdb.jdbc.JdbcConnectionAccess access = database.jdbcAccess();
                            evidence = SyncFormClient.create(JdbcSqlExecutor.create(access, dialect),
                                    JdbcBatchWriter.create(access, dialect), renderer).writeBatch(spec);
                        }
                        Set<String> expected = new HashSet<>(tokens(runtime, form, "alphabet"));
                        expected.addAll(tokens(runtime, form, "inserted"));
                        assertEquals(expected, database.tokens);
                        assertEquals(BatchExecutionState.SUCCESS, evidence.state());
                        assertEquals(1L, evidence.affectedRows().value());
                        assertEquals(1, database.acquired);
                        assertEquals(1, database.released);
                    }
                }));
            }
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> ignoredConflictsKeepExistingTokensAndOnlyInsertNewTokens() {
        List<DynamicTest> tests = new ArrayList<>();
        for (boolean reactive : List.of(false, true)) {
            for (long count : List.of(0L, 1L)) {
                tests.add(DynamicTest.dynamicTest("reactive=" + reactive + "/count=" + count,
                        () -> verifyInsertCount(reactive, true, List.of(count), 1, true)));
            }
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> ignoredConflictsDoNotGuessMissingReactiveCounts() {
        return List.of(List.<Long>of(), List.of(-2L), List.of(0L, -2L)).stream()
                .map(counts -> DynamicTest.dynamicTest("counts=" + counts,
                        () -> verifyInsertCount(true, true, counts, 1, true))).toList();
    }

    @TestFactory
    List<DynamicTest> protectedInsertsAndUpsertsDistinguishZeroFromUnknownCounts() {
        List<DynamicTest> tests = new ArrayList<>();
        for (boolean reactive : List.of(false, true)) {
            for (boolean upsert : List.of(false, true)) {
                for (long count : List.of(0L, 1L, -2L)) {
                    tests.add(DynamicTest.dynamicTest("count=" + count + "/reactive=" + reactive + "/upsert=" + upsert,
                            () -> verifyInsertCount(reactive, upsert, List.of(count))));
                }
                tests.add(DynamicTest.dynamicTest("two zero rows/reactive=" + reactive + "/upsert=" + upsert,
                        () -> verifyInsertCount(reactive, upsert, List.of(0L), 2)));
                if (reactive) {
                    tests.add(DynamicTest.dynamicTest("missing count/upsert=" + upsert,
                            () -> verifyInsertCount(true, upsert, List.of())));
                    tests.add(DynamicTest.dynamicTest("mixed unknown and zero/upsert=" + upsert,
                            () -> verifyInsertCount(true, upsert, List.of(-2L, 0L))));
                }
            }
        }
        return tests;
    }

    private static void verifyInsertCount(boolean reactive, boolean upsert, List<Long> counts) {
        verifyInsertCount(reactive, upsert, counts, 1);
    }

    private static void verifyInsertCount(boolean reactive, boolean upsert, List<Long> counts, int size) {
        verifyInsertCount(reactive, upsert, counts, size, false);
    }

    private static void verifyInsertCount(boolean reactive, boolean upsert, List<Long> counts,
                                          int size, boolean ignoreConflicts) {
        verifyInsertCount(reactive, upsert, counts, size, ignoreConflicts, false);
    }

    @TestFactory
    List<DynamicTest> conditionalUpsertsOnlyReplaceTokensForConfirmedWrites() {
        return List.of(false, true).stream().flatMap(reactive -> List.of(0L, 1L).stream()
                .map(count -> DynamicTest.dynamicTest("conditional/" + reactive + "/" + count,
                        () -> verifyInsertCount(reactive, true, List.of(count), 1, false, true)))).toList();
    }

    private static void verifyInsertCount(boolean reactive, boolean upsert, List<Long> counts,
                                          int size, boolean ignoreConflicts, boolean conditional) {
        DynamicForm form = DynamicForm.builder("sequential", "sequential")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            RdbDialect nativeDialect = RdbDialect.postgresql();
            // 未知计数属于通用驱动路径；原生 RETURNING 输入序号由专门的批量回归覆盖。
            RdbDialect dialect = ignoreConflicts || conditional ? nativeDialect : RdbDialect.of(nativeDialect.name(), nativeDialect.schema(),
                    nativeDialect.pagination(), nativeDialect.upsert()::render);
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), dialect).withProtectedFields(runtime);
            Flux<Map<String, Object>> rows = Flux.range(1, size)
                    .map(id -> Map.of("id", id.longValue(), "secret", "different"));
            BatchSpec spec = upsert ? BatchSpec.upsert(form, rows) : BatchSpec.insert(form, rows);
            if (ignoreConflicts) spec = spec.ignoreConflicts();
            if (conditional) spec = spec.withUpdateCondition(ConditionGroup.and().where("id", ">", 0L).build());
            Database database = new Database();
            database.insertCounts = counts;
            Set<String> originalTokens = tokens(runtime, form, "alphabet");
            database.tokens.addAll(originalTokens);
            List<Long> completed = new ArrayList<>();
            java.util.function.Supplier<BatchExecutionEvidence> execute;
            BatchSpec request = spec;
            if (reactive) {
                R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(database.reactiveAccess(), dialect);
                execute = () -> ReactiveFormClient.create(executor, renderer).writeBatch(request,
                                offset -> Mono.fromRunnable(() -> completed.add(offset)))
                        .block(Duration.ofSeconds(5));
            } else {
                com.flying.orm.rdb.jdbc.JdbcConnectionAccess access = database.jdbcAccess();
                execute = () -> SyncFormClient.create(JdbcSqlExecutor.create(access, dialect),
                        JdbcBatchWriter.create(access, dialect), renderer).writeBatch(request, completed::add);
            }
            boolean zero = counts.equals(List.of(0L));
            boolean known = zero || counts.equals(List.of(1L));
            if (ignoreConflicts && !known) {
                assertThrows(BatchExecutionEvidenceException.class, execute::get);
                assertEquals(originalTokens, database.tokens);
                assertEquals(0, database.sideIndexWrites);
                assertEquals(List.of(), completed);
                assertEquals(1, database.acquired);
                assertEquals(1, database.released);
                return;
            }
            BatchExecutionEvidence evidence = execute.get();
            Set<String> expected = new HashSet<>(zero || !upsert || ignoreConflicts ? originalTokens : Set.of());
            if (!zero) expected.addAll(tokens(runtime, form, "different"));
            assertAll(
                    () -> assertEquals(expected, database.tokens),
                    () -> assertEquals(!zero, database.sideIndexWrites > 0),
                    () -> assertTrue(database.businessWrites >= 1 && database.businessWrites <= size),
                    () -> assertEquals(BatchExecutionState.SUCCESS, evidence.state()),
                    () -> assertEquals(known, evidence.affectedRows().isKnown()),
                    () -> { if (known) assertEquals(zero ? 0L : 1L, evidence.affectedRows().value()); },
                    () -> assertEquals(java.util.stream.LongStream.range(0, size).boxed().toList(), completed),
                    () -> assertEquals(1, database.acquired),
                    () -> assertEquals(1, database.released));
        }
    }

    @TestFactory
    List<DynamicTest> ordinaryProtectedUpdatesRequireConfirmedOwnerWrites() {
        List<DynamicTest> tests = new ArrayList<>();
        for (boolean reactive : List.of(false, true)) {
            tests.add(DynamicTest.dynamicTest("ordinary missing owner/reactive=" + reactive,
                    () -> verifyOrdinary(reactive, false, List.of(0L))));
            for (long count : List.of(0L, 1L, (long) java.sql.Statement.SUCCESS_NO_INFO)) {
                tests.add(DynamicTest.dynamicTest("ordinary count=" + count + "/reactive=" + reactive,
                        () -> verifyOrdinary(reactive, true, List.of(count))));
            }
        }
        tests.add(DynamicTest.dynamicTest("ordinary reactive mixed unknown and known counts",
                () -> verifyOrdinary(true, true, List.of(-2L, 1L))));
        tests.add(DynamicTest.dynamicTest("ordinary reactive missing count",
                () -> verifyOrdinary(true, true, List.of())));
        return tests;
    }

    @TestFactory
    List<DynamicTest> jdbcMixedInsertCountsOnlyMaintainWrittenRows() {
        List<DynamicTest> tests = new ArrayList<>();
        for (boolean upsert : List.of(false, true)) {
            for (List<Long> counts : List.of(List.of(1L, 0L), List.of(0L, 1L),
                    List.of(-2L, 0L), List.of(0L, -2L))) {
                tests.add(DynamicTest.dynamicTest("mixed counts=" + counts + "/upsert=" + upsert,
                        () -> verifyMixedJdbcInsertCount(upsert, counts)));
            }
        }
        return tests;
    }

    private static void verifyMixedJdbcInsertCount(boolean upsert, List<Long> counts) {
        DynamicForm form = DynamicForm.builder("sequential", "sequential")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            RdbDialect nativeDialect = RdbDialect.postgresql();
            RdbDialect dialect = RdbDialect.of(nativeDialect.name(), nativeDialect.schema(),
                    nativeDialect.pagination(), nativeDialect.upsert()::render);
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), dialect).withProtectedFields(runtime);
            Flux<Map<String, Object>> rows = Flux.just(Map.of("id", 1L, "secret", "alphabet"),
                    Map.of("id", upsert ? 1L : 2L, "secret", "different"));
            BatchSpec spec = upsert ? BatchSpec.upsert(form, rows) : BatchSpec.insert(form, rows);
            Database database = new Database();
            database.insertRowCounts = counts;
            List<Long> completed = new ArrayList<>();
            com.flying.orm.rdb.jdbc.JdbcConnectionAccess access = database.jdbcAccess();
            BatchExecutionEvidence evidence = SyncFormClient.create(JdbcSqlExecutor.create(access, dialect),
                    JdbcBatchWriter.create(access, dialect), renderer).writeBatch(spec, completed::add);
            Set<String> expected = tokens(runtime, form, counts.getFirst() == 0 ? "different" : "alphabet");
            assertAll(
                    () -> assertEquals(expected, database.tokens),
                    () -> assertEquals(expected.size() + (upsert ? 1 : 0), database.sideIndexWrites),
                    () -> assertEquals(2, database.businessWrites),
                    () -> assertEquals(BatchExecutionState.SUCCESS, evidence.state()),
                    () -> assertEquals(!counts.contains(-2L), evidence.affectedRows().isKnown()),
                    () -> { if (!counts.contains(-2L)) assertEquals(1L, evidence.affectedRows().value()); },
                    () -> assertEquals(List.of(0L, 1L), completed),
                    () -> assertEquals(1, database.acquired),
                    () -> assertEquals(1, database.released));
        }
    }

    private static void verifyOrdinary(boolean reactive, boolean ownerPresent, List<Long> counts) {
        DynamicForm form = DynamicForm.builder("sequential", "sequential")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            RdbDialect dialect = RdbDialect.postgresql();
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), dialect).withProtectedFields(runtime);
            BatchSpec spec = BatchSpec.update(form, Flux.just(new BatchUpdate(Map.of("secret", "different"),
                    ConditionGroup.and().where("id", "=", 1L).build())));
            Database database = new Database();
            database.ordinaryCounts = counts;
            database.ownerPresent = ownerPresent;
            Set<String> originalTokens = tokens(runtime, form, "alphabet");
            database.tokens.addAll(originalTokens);
            List<Long> completed = new ArrayList<>();
            java.util.function.Supplier<BatchExecutionEvidence> execute;
            if (reactive) {
                R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(database.reactiveAccess(), dialect);
                execute = () -> ReactiveFormClient.create(executor, renderer).writeBatch(spec,
                                offset -> Mono.fromRunnable(() -> completed.add(offset)))
                        .block(Duration.ofSeconds(5));
            } else {
                com.flying.orm.rdb.jdbc.JdbcConnectionAccess access = database.jdbcAccess();
                execute = () -> SyncFormClient.create(JdbcSqlExecutor.create(access, dialect),
                        JdbcBatchWriter.create(access, dialect), renderer).writeBatch(spec, completed::add);
            }
            boolean updated = ownerPresent && counts.equals(List.of(1L));
            boolean completedNormally = !ownerPresent || updated;
            BatchExecutionEvidence evidence = completedNormally ? execute.get()
                    : assertThrows(BatchExecutionEvidenceException.class, execute::get).evidence();
            assertAll(
                    () -> assertEquals(updated ? tokens(runtime, form, "different") : originalTokens,
                            database.tokens, "unconfirmed main writes must not replace search tokens"),
                    () -> assertEquals(updated, database.sideIndexWrites > 0),
                    () -> assertEquals(ownerPresent ? 1 : 0, database.businessWrites),
                    () -> assertEquals(completedNormally ? List.of(0L) : List.of(), completed),
                    () -> assertEquals(List.of(), evidence.conflicts()),
                    () -> assertEquals(!ownerPresent || counts.equals(List.of(0L)) || updated,
                            evidence.affectedRows().isKnown()),
                    () -> assertEquals(1, database.acquired),
                    () -> assertEquals(1, database.released));
        }
    }

    @TestFactory
    List<DynamicTest> sequentialUpdatesDoNotDependOnInputBufferSize() {
        List<DynamicTest> tests = new ArrayList<>();
        for (boolean reactive : List.of(false, true)) {
            for (boolean protectedField : List.of(false, true)) {
                for (int buffer : List.of(1, 2)) {
                    tests.add(DynamicTest.dynamicTest(
                            "reactive=" + reactive + "/protected=" + protectedField + "/buffer=" + buffer,
                            () -> verify(reactive, protectedField, buffer, false)));
                }
            }
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> laterVersionConflictKeepsTheCompletedProtectedRow() {
        return List.of(false, true).stream().map(reactive -> DynamicTest.dynamicTest(
                "version conflict/reactive=" + reactive, () -> verify(reactive, true, 2, true))).toList();
    }

    private static void verify(boolean reactive, boolean protectedField, int buffer, boolean conflict) {
        var builder = DynamicForm.builder("sequential", "sequential")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("version", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"));
        if (protectedField) {
            builder.encrypted("secret", EncryptedFieldDefinition.builder()
                    .searchModes(EncryptedSearchMode.CONTAINS).build());
        }
        DynamicForm form = builder.build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            RdbDialect dialect = RdbDialect.postgresql();
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), dialect).withProtectedFields(runtime);
            BatchSpec spec = BatchSpec.update(form, Flux.just(
                    update("alphabet", 1L), update("different", conflict ? 9L : 2L)))
                    .withOptions(BatchWriteOptions.of(buffer));
            Database database = new Database();
            List<Long> completed = new ArrayList<>();
            java.util.function.Supplier<BatchExecutionEvidence> execute;
            if (reactive) {
                var executor = R2dbcSqlExecutor.create(database.reactiveAccess(), dialect);
                execute = () -> ReactiveFormClient.create(executor, renderer).writeBatch(spec,
                                offset -> Mono.fromRunnable(() -> completed.add(offset)))
                        .block(Duration.ofSeconds(5));
            } else {
                var access = database.jdbcAccess();
                execute = () -> SyncFormClient.create(JdbcSqlExecutor.create(access, dialect),
                        JdbcBatchWriter.create(access, dialect), renderer).writeBatch(spec, completed::add);
            }
            BatchExecutionEvidence result = conflict
                    ? assertThrows(BatchExecutionEvidenceException.class, execute::get).evidence() : execute.get();
            Set<String> expectedTokens = tokens(runtime, form, conflict ? "alphabet" : "different");
            List<Set<String>> expectedBeforeReads = protectedField
                    ? List.of(Set.of(), tokens(runtime, form, "alphabet")) : List.of();
            assertAll(
                    () -> assertEquals(conflict ? BatchExecutionState.PARTIAL : BatchExecutionState.SUCCESS,
                            result.state()),
                    () -> assertEquals(conflict ? 1L : 2L, result.successfulCount()),
                    () -> assertEquals(conflict ? 2L : 3L, database.version),
                    () -> assertEquals(conflict ? 1 : 2, database.businessWrites),
                    () -> assertEquals(conflict ? List.of(0L) : List.of(0L, 1L), completed),
                    () -> assertEquals(1, database.acquired),
                    () -> assertEquals(1, database.released),
                    () -> assertEquals(expectedBeforeReads, database.tokensBeforeOwnerReads,
                            "each prior row must finish token replacement before the next owner query"),
                    () -> assertEquals(expectedTokens, database.tokens));
        }
    }

    private static Set<String> tokens(ProtectedFieldRuntime runtime, DynamicForm form, String value) {
        Set<String> result = new HashSet<>();
        runtime.writeOperation(form, ProtectedFormLayout.physical(form), DataScope.none(), ValueCodecRegistry.standard())
                .containsTokens(Map.of("secret", value)).forEach(field ->
                field.tokens().forEach(token -> result.add(binary(token))));
        return result;
    }

    private static BatchUpdate update(String value, long expectedVersion) {
        return new BatchUpdate(Map.of("secret", value),
                ConditionGroup.and().where("id", "=", 1L).build(),
                OptimisticLockOptions.increment("version", expectedVersion));
    }

    /** Models only the SQL used by this test; the ORM owns all planning and execution sequencing. */
    private static final class Database {
        private long version = 1L;
        private int businessWrites;
        private int acquired;
        private int released;
        private List<Long> ordinaryCounts;
        private List<Long> insertCounts = List.of(0L);
        private List<Long> insertRowCounts;
        private boolean aggregateInsertCounts;
        private boolean ownerPresent = true;
        private int sideIndexWrites;
        private final Set<String> tokens = new HashSet<>();
        private final List<Set<String>> tokensBeforeOwnerReads = new ArrayList<>();

        private com.flying.orm.rdb.jdbc.JdbcConnectionAccess jdbcAccess() {
            return new com.flying.orm.rdb.jdbc.JdbcConnectionAccess() {
                public java.sql.Connection getConnection(com.flying.orm.core.sql.render.SqlRequest request) {
                    acquired++;
                    return jdbc();
                }
                public void releaseConnection(java.sql.Connection connection,
                                              com.flying.orm.core.sql.render.SqlRequest request) {
                    released++;
                }
            };
        }

        private com.flying.orm.rdb.reactive.R2dbcConnectionAccess reactiveAccess() {
            return new com.flying.orm.rdb.reactive.R2dbcConnectionAccess() {
                public Mono<io.r2dbc.spi.Connection> getConnection(
                        com.flying.orm.core.sql.render.SqlRequest request) {
                    return Mono.fromSupplier(() -> { acquired++; return reactive(); });
                }
                public Mono<Void> releaseConnection(reactor.core.publisher.SignalType signal,
                        io.r2dbc.spi.Connection connection, com.flying.orm.core.sql.render.SqlRequest request) {
                    return Mono.fromRunnable(() -> released++);
                }
            };
        }

        private List<Object[]> owners(String sql, List<Object> parameters) {
            List<Object[]> rows = new ArrayList<>();
            boolean slots = sql.contains("flying_owner_");
            int width = ordinaryCounts == null ? 2 : 1;
            for (int offset = 0; offset < parameters.size(); offset += width) {
                tokensBeforeOwnerReads.add(Set.copyOf(tokens));
                long expected = ((Number) parameters.get(offset + width - 1)).longValue();
                if (ordinaryCounts == null ? expected == version : ownerPresent) {
                    rows.add(slots ? new Object[]{1L, offset / width} : new Object[]{1L});
                }
            }
            return rows;
        }

        private int write(String sql, List<Object> parameters) {
            if (sql.startsWith("insert into \"sequential\" ") || sql.startsWith("merge into ")) {
                businessWrites++;
                if (insertRowCounts != null) return insertRowCounts.get(businessWrites - 1).intValue();
                return insertCounts.isEmpty() ? 0 : insertCounts.getFirst().intValue();
            }
            if (sql.startsWith("update ")) {
                if (ordinaryCounts != null) {
                    businessWrites++;
                    return ordinaryCounts.isEmpty() ? 0 : ordinaryCounts.getFirst().intValue();
                }
                // SET secret=?, WHERE id=? AND version=?; an owner restriction may follow.
                long expected = ((Number) parameters.get(2)).longValue();
                if (expected != version) return 0;
                version++;
                businessWrites++;
                return 1;
            }
            if (sql.startsWith("delete ")) {
                sideIndexWrites++;
                int count = tokens.size();
                tokens.clear();
                return count;
            }
            if (sql.startsWith("insert ")) {
                sideIndexWrites++;
                tokens.add(binary(parameters.getLast()));
                return 1;
            }
            throw new AssertionError("unexpected SQL: " + sql);
        }

        private java.sql.Connection jdbc() {
            return proxy(java.sql.Connection.class, (self, method, args) -> {
                if (method.getName().equals("prepareStatement")) return jdbcStatement((String) args[0]);
                throw new AssertionError("unexpected connection call: " + method);
            });
        }

        private PreparedStatement jdbcStatement(String sql) {
            Map<Integer, Object> bindings = new TreeMap<>();
            List<List<Object>> batch = new ArrayList<>();
            return proxy(PreparedStatement.class, (self, method, args) -> switch (method.getName()) {
                case "setObject", "setBinaryStream" -> {
                    Object value = args[1] instanceof InputStream input ? input.readAllBytes() : args[1];
                    bindings.put((Integer) args[0], value);
                    yield null;
                }
                case "addBatch" -> { batch.add(new ArrayList<>(bindings.values())); yield null; }
                case "executeBatch" -> {
                    int[] counts = batch.stream().mapToInt(row -> write(sql, row)).toArray();
                    if (aggregateInsertCounts && sql.startsWith("merge into ")) {
                        java.util.Arrays.fill(counts, java.sql.Statement.SUCCESS_NO_INFO);
                    }
                    batch.clear();
                    yield counts;
                }
                case "executeUpdate" -> write(sql, new ArrayList<>(bindings.values()));
                case "executeLargeUpdate" -> (long) write(sql, new ArrayList<>(bindings.values()));
                case "executeQuery" -> jdbcRows(owners(sql, new ArrayList<>(bindings.values())),
                        sql.contains("flying_owner_") ? 2 : 1);
                case "close", "cancel" -> null;
                case "isClosed" -> false;
                default -> throw new AssertionError("unexpected statement call: " + method);
            });
        }

        private io.r2dbc.spi.Connection reactive() {
            return proxy(io.r2dbc.spi.Connection.class, (self, method, args) -> {
                if (method.getName().equals("createStatement")) return reactiveStatement((String) args[0]);
                throw new AssertionError("unexpected connection call: " + method);
            });
        }

        private io.r2dbc.spi.Statement reactiveStatement(String sql) {
            Map<Integer, Object> bindings = new TreeMap<>();
            List<List<Object>> batch = new ArrayList<>();
            return proxy(io.r2dbc.spi.Statement.class, (self, method, args) -> switch (method.getName()) {
                case "bind" -> { bindings.put((Integer) args[0], args[1]); yield self; }
                case "fetchSize" -> self;
                case "add" -> { batch.add(new ArrayList<>(bindings.values())); yield self; }
                case "execute" -> Flux.defer(() -> {
                    if (sql.startsWith("select ")) {
                        return Flux.just(reactiveRows(owners(sql, new ArrayList<>(bindings.values())),
                                sql.contains("flying_owner_") ? 2 : 1));
                    }
                    batch.add(new ArrayList<>(bindings.values()));
                    if (aggregateInsertCounts && sql.startsWith("merge into ")) {
                        return Flux.just(updated(batch.stream().mapToInt(row -> write(sql, row)).sum()));
                    }
                    return Flux.fromIterable(batch).map(row -> {
                        int count = write(sql, row);
                        if (sql.startsWith("insert into \"sequential\" ")) return updated(insertCounts);
                        return sql.startsWith("update ") && ordinaryCounts != null
                                ? updated(ordinaryCounts) : updated(count);
                    });
                });
                default -> throw new AssertionError("unexpected statement call: " + method);
            });
        }
    }

    private static ResultSet jdbcRows(List<Object[]> rows, int columns) {
        ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (self, method, args) -> switch (method.getName()) {
            case "getColumnCount" -> columns;
            case "getColumnLabel", "getColumnName" -> (Integer) args[0] == 1 ? "id" : "__flying_owner_slot";
            case "getColumnType" -> Types.BIGINT;
            case "getColumnTypeName" -> "BIGINT";
            case "getColumnClassName" -> Long.class.getName();
            default -> null;
        });
        int[] cursor = {-1};
        return proxy(ResultSet.class, (self, method, args) -> switch (method.getName()) {
            case "getMetaData" -> metadata;
            case "next" -> ++cursor[0] < rows.size();
            case "getObject" -> rows.get(cursor[0])[(Integer) args[0] - 1];
            case "wasNull", "isClosed" -> false;
            case "close" -> null;
            default -> throw new AssertionError("unexpected row call: " + method);
        });
    }

    @SuppressWarnings("unchecked")
    private static Result reactiveRows(List<Object[]> rows, int columns) {
        List<ColumnMetadata> metadataColumns = new ArrayList<>();
        for (int index = 0; index < columns; index++) {
            String name = index == 0 ? "id" : "__flying_owner_slot";
            metadataColumns.add(proxy(ColumnMetadata.class, (self, method, args) -> switch (method.getName()) {
                case "getName" -> name;
                case "getJavaType" -> Long.class;
                case "getType" -> R2dbcType.BIGINT;
                case "getNullability" -> io.r2dbc.spi.Nullability.NON_NULL;
                default -> null;
            }));
        }
        RowMetadata metadata = proxy(RowMetadata.class, (self, method, args) -> switch (method.getName()) {
            case "getColumnMetadatas" -> metadataColumns;
            case "getColumnMetadata" -> metadataColumns.get((Integer) args[0]);
            default -> throw new AssertionError(method);
        });
        return proxy(Result.class, (self, method, args) -> {
            if (!method.getName().equals("map")) throw new AssertionError(method);
            return Flux.fromIterable(rows).map(values -> {
                Row row = proxy(Row.class, (rowSelf, rowMethod, rowArgs) -> switch (rowMethod.getName()) {
                    case "get" -> values[(Integer) rowArgs[0]];
                    case "getMetadata" -> metadata;
                    default -> throw new AssertionError(rowMethod);
                });
                return ((BiFunction<Row, RowMetadata, Object>) args[0]).apply(row, metadata);
            });
        });
    }

    private static Result updated(long count) {
        return updated(List.of(count));
    }

    private static Result updated(List<Long> counts) {
        return proxy(Result.class, (self, method, args) -> {
            if (method.getName().equals("getRowsUpdated")) return Flux.fromIterable(counts);
            throw new AssertionError(method);
        });
    }

    private static String binary(Object value) {
        if (value instanceof byte[] bytes) return Base64.getEncoder().encodeToString(bytes);
        ByteBuffer buffer = ((ByteBuffer) value).duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
