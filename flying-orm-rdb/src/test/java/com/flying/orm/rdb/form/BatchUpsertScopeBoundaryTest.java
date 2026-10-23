package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchUpsertScopeBoundaryTest {

    @TestFactory
    Stream<DynamicTest> businessUpdateConditionUsesTheSharedBatchPathWithoutBecomingScope() {
        return dialects().filter(dialect -> !dialect.name().equals("mysql"))
                .flatMap(dialect -> Stream.of(false, true).flatMap(reactive -> Stream.of(false, true)
                        .map(scoped -> DynamicTest.dynamicTest(dialect.name() + "/condition/" + reactive + "/" + scoped, () -> {
                            ConditionGroup condition = ConditionGroup.and().where("name", "=", "old").build();
                            BatchSpec original = BatchSpec.upsert(form(), Flux.just(row(7L), row(8L)));
                            BatchSpec spec = original.withUpdateCondition(condition).onConflict("id")
                                    .ignoreUpdates("org_id").withOptions(BatchWriteOptions.of(2))
                                    .withGeneratedKeys(com.flying.orm.rdb.batch.BatchGeneratedKeys.none())
                                    .withScope(scoped ? rowScope() : DataScope.none());
                            assertTrue(original.updateCondition().children().isEmpty());
                            assertEquals(condition, spec.updateCondition());
                            CapturingWriter capture = new CapturingWriter();
                            execute(capture, dialect, DataScope.none(), spec, reactive, "result");
                            assertEquals(scoped ? List.of(7L, "scope-A", "changed", "scope-A", "old")
                                    : List.of(7L, "scope-A", "changed", "old"), capture.rows.getFirst());
                            assertEquals(scoped ? List.of(8L, "scope-A", "changed", "scope-A", "old")
                                    : List.of(8L, "scope-A", "changed", "old"), capture.rows.getLast());
                            String sql = capture.sql.replace("\"", "").replace("[", "").replace("]", "");
                            assertTrue(sql.contains("target.name = ?"), sql);
                            assertEquals(scoped, sql.contains("scope violation"), sql);
                            if (dialect.name().equals("postgresql") && !scoped) {
                                assertEquals("insert into scoped_rows as target (id, org_id, name) values (?, ?, ?) "
                                        + "on conflict (id) do update set name = excluded.name where target.name = ?", sql);
                            }
                        }))));
    }

    @org.junit.jupiter.api.Test
    void updateConditionSurvivesCopiesButIsUnusedWhenConflictUpdatesAreDisabled() {
        ConditionGroup condition = ConditionGroup.and().where("name", "=", "old").build();
        BatchSpec spec = BatchSpec.upsert(form(), Flux.just(row(7L))).withUpdateCondition(condition).ignoreConflicts();
        assertEquals(condition, spec.updateCondition());
        CapturingWriter capture = new CapturingWriter();
        execute(capture, RdbDialect.postgresql(), DataScope.none(), spec, false, "result");
        assertTrue(capture.sql.endsWith("do nothing"));
        assertEquals(List.of(List.of(7L, "scope-A", "changed")), capture.rows);
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> BatchSpec.insert(form(), Flux.empty()).withUpdateCondition(condition));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> BatchSpec.update(form(), Flux.empty()).withUpdateCondition(condition));
    }

    @TestFactory
    Stream<DynamicTest> mysqlRejectsConditionalUpdatesRatherThanEmulatingThemWithSelfAssignments() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("mysql/condition/" + reactive, () -> {
            CapturingWriter capture = new CapturingWriter();
            BatchSpec spec = BatchSpec.upsert(form(), Flux.just(row(7L)))
                    .withUpdateCondition(ConditionGroup.and().where("name", "=", "old").build());
            org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> execute(capture, RdbDialect.mysql(), DataScope.none(), spec, reactive, "result"));
            assertEquals(0, capture.executions);
        }));
    }

    @TestFactory
    Stream<DynamicTest> conditionalUpsertRespectsExplicitFilterPermissions() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("condition-permission/" + reactive, () -> {
            CapturingWriter capture = new CapturingWriter();
            BatchSpec spec = BatchSpec.upsert(form(), Flux.just(row(7L)))
                    .withUpdateCondition(ConditionGroup.and().where("name", "=", "old").build());
            com.flying.orm.core.scope.FieldUsePolicy policy = com.flying.orm.core.scope.FieldUsePolicy.builder()
                    .allow("id", com.flying.orm.core.scope.FieldUse.INSERT)
                    .allow("org_id", com.flying.orm.core.scope.FieldUse.INSERT, com.flying.orm.core.scope.FieldUse.UPDATE)
                    .allow("name", com.flying.orm.core.scope.FieldUse.INSERT, com.flying.orm.core.scope.FieldUse.UPDATE).build();
            org.junit.jupiter.api.Assertions.assertThrows(com.flying.orm.core.scope.ScopeAccessException.class,
                    () -> execute(capture, RdbDialect.postgresql(), DataScope.none(), spec, reactive, "result", policy));
            assertEquals(0, capture.executions);
        }));
    }

    @TestFactory
    Stream<DynamicTest> explicitBusinessKeyDoesNotRequireAPrimaryKeyOrUpdatePermissionOnTheKey() {
        return dialects().flatMap(dialect -> Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                dialect.name() + "/business-key/" + reactive, () -> {
                    DynamicForm form = DynamicForm.builder("accounts", "accounts")
                            .addField(DynamicField.of("email", "VARCHAR"))
                            .addField(DynamicField.of("name", "VARCHAR")).build();
                    Map<String, Object> first = new LinkedHashMap<>();
                    first.put("email", "a@example.test");
                    first.put("name", "first");
                    Map<String, Object> second = new LinkedHashMap<>();
                    second.put("name", "second");
                    second.put("email", "b@example.test");
                    BatchSpec original = BatchSpec.upsert(form, Flux.just(first, second));
                    BatchSpec spec = original.onConflict(" EMAIL ").withScope(DataScope.none())
                            .withOptions(BatchWriteOptions.of(2));
                    assertTrue(original.conflictFields().isEmpty());
                    assertEquals(List.of("email"), spec.conflictFields());
                    CapturingWriter capture = new CapturingWriter();
                    execute(capture, dialect, DataScope.none(), spec, reactive, "result",
                            com.flying.orm.core.scope.FieldUsePolicy.builder()
                                    .allow("email", com.flying.orm.core.scope.FieldUse.INSERT)
                                    .allow("name", com.flying.orm.core.scope.FieldUse.INSERT,
                                            com.flying.orm.core.scope.FieldUse.UPDATE).build());
                    assertEquals(List.of(List.of("a@example.test", "first"),
                            List.of("b@example.test", "second")), capture.rows);
                    assertEquals(1, capture.executions);
                    String sql = capture.sql.replace("\"", "").replace("`", "").replace("[", "").replace("]", "");
                    if (dialect.name().equals("postgresql")) {
                        assertTrue(sql.contains("on conflict (email) do update set name = excluded.name"), sql);
                    } else if (dialect.name().equals("mysql")) {
                        assertTrue(sql.contains("email = values(email)"), sql);
                    } else {
                        assertTrue(sql.contains("target.email = source.email"), sql);
                    }
                })));
    }

    @org.junit.jupiter.api.Test
    void explicitConflictChoiceSurvivesCopiesAndCanBeResetWithoutRetainingCallerArray() {
        String[] fields = {" NAME ", "name", "org_id"};
        BatchSpec original = BatchSpec.upsert(form(), Flux.just(row(7L)));
        BatchSpec spec = original.onConflict(fields).ignoreUpdates("id").ignoreConflicts()
                .withScope(rowScope()).withOptions(BatchWriteOptions.of(2))
                .withGeneratedKeys(com.flying.orm.rdb.batch.BatchGeneratedKeys.none());
        fields[0] = "id";
        assertEquals(List.of("name", "org_id"), spec.conflictFields());
        org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
                () -> spec.conflictFields().clear());
        assertTrue(spec.onConflict().conflictFields().isEmpty());
        assertEquals(List.of("id"), spec.onConflict("id").conflictFields());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> BatchSpec.insert(form(), Flux.empty()).onConflict("name"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> BatchSpec.update(form(), Flux.empty()).onConflict("name"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> original.onConflict("missing"));
    }

    @TestFactory
    Stream<DynamicTest> explicitConflictKeyMustBePresentBeforeEitherExecutorIsCalled() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("missing-key/" + reactive, () -> {
            CapturingWriter capture = new CapturingWriter();
            BatchSpec spec = BatchSpec.upsert(form(), Flux.just(Map.<String, Object>of("id", 1L, "name", "new")))
                    .onConflict("org_id");
            IllegalArgumentException error = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                    () -> execute(capture, RdbDialect.postgresql(), DataScope.none(), spec, reactive, "result"));
            assertTrue(error.getMessage().contains("all conflict key fields"));
            assertEquals(0, capture.executions);
        }));
    }

    @TestFactory
    Stream<DynamicTest> explicitBusinessKeyIncludesAutoTenantAndRetainsConflictIgnoreChoice() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(ignore -> DynamicTest.dynamicTest(
                "tenant-business-key/" + reactive + "/" + ignore, () -> {
                    DynamicForm form = DynamicForm.builder("accounts", "accounts")
                            .addField(DynamicField.of("email", "VARCHAR"))
                            .addField(DynamicField.of("org_id", "VARCHAR"))
                            .addField(DynamicField.of("name", "VARCHAR"))
                            .tenant("org_id", com.flying.orm.core.form.TenantStrategy.AUTO).build();
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("email", "a@example.test");
                    row.put("name", "new");
                    BatchSpec spec = BatchSpec.upsert(form, Flux.just(row)).onConflict("org_id", "email");
                    CapturingWriter capture = new CapturingWriter();
                    execute(capture, RdbDialect.postgresql(), DataScope.tenant("org_id", "tenant-A"),
                            ignore ? spec.ignoreConflicts() : spec, reactive, "evidence");
                    assertTrue(capture.sql.contains("on conflict (\"org_id\", \"email\")"), capture.sql);
                    assertTrue(capture.rows.getFirst().containsAll(List.of("a@example.test", "new", "tenant-A")));
                    assertEquals(ignore ? 3 : 4, capture.parameterCount);
                    if (ignore) assertTrue(capture.sql.endsWith("do nothing"), capture.sql);
                    else {
                        assertTrue(capture.sql.contains("do update set \"name\" = excluded.\"name\" where "), capture.sql);
                        assertEquals("tenant-A", capture.rows.getFirst().getLast());
                    }
                })));
    }

    @org.junit.jupiter.api.Test
    void encryptedConflictKeysAndMismatchedContainsOwnersAreRejected() {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("email", "VARCHAR"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", com.flying.orm.core.protection.EncryptedFieldDefinition.builder()
                        .searchModes(com.flying.orm.core.protection.EncryptedSearchMode.CONTAINS).build()).build();
        BatchSpec spec = BatchSpec.upsert(form, Flux.empty());
        assertTrue(org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> spec.onConflict("secret")).getMessage().contains("encrypted field"));
        assertTrue(org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> spec.onConflict("email")).getMessage().contains("all primary keys"));
        assertEquals(List.of("email", "id"), spec.onConflict("email", "id").conflictFields());
    }

    @org.junit.jupiter.api.Test
    void conflictIgnoreIsAnImmutableUpsertOnlyOption() {
        BatchSpec original = BatchSpec.upsert(form(), Flux.just(row(7L)));
        BatchSpec ignored = original.ignoreConflicts();
        assertFalse(original.ignoresConflicts());
        assertTrue(ignored.ignoresConflicts());
        assertTrue(ignored.withOptions(BatchWriteOptions.of(2)).withScope(rowScope())
                .withGeneratedKeys(com.flying.orm.rdb.batch.BatchGeneratedKeys.none()).ignoresConflicts());
        org.junit.jupiter.api.Assertions.assertSame(original.rows(), ignored.rows());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> BatchSpec.insert(form(), Flux.just(row(7L))).ignoreConflicts());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> BatchSpec.update(form(), Flux.empty()).ignoreConflicts());
    }

    @TestFactory
    Stream<DynamicTest> ignoreConflictsUsesOnlyInsertPermissionsAndRetainsTenantValues() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of("result", "evidence")
                .map(channel -> DynamicTest.dynamicTest(reactive + "/" + channel, () -> {
                    CapturingWriter capture = new CapturingWriter();
                    AtomicInteger subscriptions = new AtomicInteger();
                    DynamicForm tenantForm = DynamicForm.builder("scoped_rows", "scoped_rows")
                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                            .addField(DynamicField.of("org_id", "VARCHAR"))
                            .addField(DynamicField.of("name", "VARCHAR"))
                            .tenant("org_id", com.flying.orm.core.form.TenantStrategy.AUTO).build();
                    BatchSpec spec = BatchSpec.upsert(tenantForm, Flux.defer(() -> {
                        subscriptions.incrementAndGet();
                        return Flux.just(Map.<String, Object>of("id", 7L, "name", "first"),
                                Map.<String, Object>of("id", 8L, "name", "last"));
                    })).ignoreConflicts().withOptions(BatchWriteOptions.of(2)).withScope(rowScope());
                    execute(capture, RdbDialect.postgresql(), DataScope.tenant("org_id", "scope-A"),
                            spec, reactive, channel, com.flying.orm.core.scope.FieldUsePolicy.builder()
                                    .allow("id", com.flying.orm.core.scope.FieldUse.INSERT)
                                    .allow("name", com.flying.orm.core.scope.FieldUse.INSERT).build());
                    assertEquals(1, subscriptions.get());
                    assertEquals(1, capture.executions);
                    assertTrue(capture.sql.endsWith("on conflict (\"id\") do nothing"), capture.sql);
                    assertFalse(capture.sql.contains("where"));
                    assertEquals(3, capture.parameterCount);
                    assertEquals(2, capture.rows.size());
                    assertTrue(capture.rows.get(0).containsAll(List.of(7L, "first", "scope-A")));
                    assertTrue(capture.rows.get(1).containsAll(List.of(8L, "last", "scope-A")));
                })));
    }

    @TestFactory
    Stream<DynamicTest> mergeConflictIgnorePreservesInsertOnlyPlanning() {
        return Stream.of(RdbDialect.oracle(), RdbDialect.sqlServer(), RdbDialect.h2())
                .flatMap(dialect -> Stream.of(false, true).flatMap(reactive -> Stream.of("result", "evidence")
                        .map(channel -> DynamicTest.dynamicTest(dialect.name() + "/" + reactive + "/" + channel, () -> {
                            CapturingWriter capture = new CapturingWriter();
                            BatchSpec spec = BatchSpec.upsert(form(), Flux.just(row(7L), row(8L)))
                                    .ignoreConflicts().withOptions(BatchWriteOptions.of(2)).withScope(rowScope());
                            execute(capture, dialect, DataScope.none(), spec, reactive, channel,
                                    com.flying.orm.core.scope.FieldUsePolicy.builder()
                                            .allow("id", com.flying.orm.core.scope.FieldUse.INSERT)
                                            .allow("org_id", com.flying.orm.core.scope.FieldUse.INSERT)
                                            .allow("name", com.flying.orm.core.scope.FieldUse.INSERT).build());
                            assertEquals(1, capture.executions);
                            assertTrue(capture.sql.startsWith("merge into"), capture.sql);
                            assertTrue(capture.sql.contains("when not matched then insert"), capture.sql);
                            assertFalse(capture.sql.contains("when matched"), capture.sql);
                            assertFalse(capture.sql.contains("scope violation"), capture.sql);
                            assertEquals(3, capture.parameterCount);
                            assertEquals(List.of(List.of(7L, "scope-A", "changed"),
                                    List.of(8L, "scope-A", "changed")), capture.rows);
                            assertFalse(((com.flying.orm.rdb.internal.dialect.StagedUpsertDialect) dialect.upsert())
                                    .supportsBatchRowIndex());
                        }))));
    }

    @TestFactory
    Stream<DynamicTest> unsupportedConflictIgnoreNeverReachesExecution() {
        return Stream.of(RdbDialect.mysql())
                .flatMap(dialect -> Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                        dialect.name() + "/" + reactive, () -> {
                            CapturingWriter capture = new CapturingWriter();
                            org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
                                    () -> execute(capture, dialect, DataScope.none(),
                                            BatchSpec.upsert(form(), Flux.just(row(7L))).ignoreConflicts(),
                                            reactive, "result"));
                            assertEquals(0, capture.executions);
                        })));
    }

    @TestFactory
    Stream<DynamicTest> ignoreConflictsDropsUpdateOnlyEntityFieldsOnEveryRow() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("reactive=" + reactive, () -> {
            CapturingWriter capture = new CapturingWriter();
            DynamicForm form = DynamicForm.builder("ignored_stage", "ignored_stage")
                    .addField(DynamicField.primaryKey("id", "BIGINT"))
                    .addField(DynamicField.of("name", "VARCHAR")).build();
            com.flying.orm.rdb.internal.mapping.EntityValues entityValues =
                    com.flying.orm.rdb.internal.mapping.EntityValues.createUncached(IgnoredStageEntity.class);
            Map<String, Object> first = entityValues.repositoryUpsertValues(new IgnoredStageEntity(7L, "first"));
            Map<String, Object> second = entityValues.repositoryUpsertValues(new IgnoredStageEntity(8L, "last"));
            execute(capture, RdbDialect.postgresql(), DataScope.none(),
                    BatchSpec.upsert(form, Flux.just(first, second)).ignoreConflicts()
                            .withScope(DataScope.none().withFields(FieldScope.writable("id"))), reactive, "evidence");
            assertEquals(List.of(List.of(7L), List.of(8L)), capture.rows);
            assertFalse(capture.sql.contains("name"));
            assertTrue(first.containsKey("name"), "the source snapshot must stay intact");
        }));
    }

    @com.flying.orm.core.annotation.TableName("ignored_stage")
    private static final class IgnoredStageEntity {
        @com.flying.orm.core.annotation.TableId(type = com.flying.orm.core.annotation.IdType.INPUT)
        private final Long id;
        @com.flying.orm.core.annotation.TableField(insertStrategy = com.flying.orm.core.annotation.FieldStrategy.NEVER)
        private final String name;

        private IgnoredStageEntity(Long id, String name) { this.id = id; this.name = name; }
        public Long getId() { return id; }
        public String getName() { return name; }
    }

    @TestFactory
    Stream<DynamicTest> allPublicChannelsBindTheEffectiveTargetScopeOnEveryRow() {
        return dialects().flatMap(dialect -> Stream.of(false, true).flatMap(reactive ->
                Stream.of("default", "explicit", "tenant", "merged").map(source -> DynamicTest.dynamicTest(
                        dialect.name() + " reactive=" + reactive + " scope=" + source, () -> {
                            String expectedSql = null;
                            for (String channel : List.of("result", "evidence")) {
                                CapturingWriter capture = new CapturingWriter();
                                AtomicInteger subscriptions = new AtomicInteger();
                                DataScope initial = source.equals("explicit") ? DataScope.none()
                                        : source.equals("tenant") ? DataScope.tenant("org_id", "scope-A") : rowScope();
                                DataScope explicit = source.equals("explicit") ? rowScope()
                                        : source.equals("merged") ? DataScope.where(ConditionGroup.and()
                                                .where("name", "=", "scope-old-name").build()) : DataScope.all();
                                BatchSpec spec = BatchSpec.upsert(form(), Flux.defer(() -> {
                                    subscriptions.incrementAndGet();
                                    return Flux.just(row(7L), row(8L));
                                })).withScope(explicit).withOptions(channel.equals("chunks")
                                        ? BatchWriteOptions.of(1) : BatchWriteOptions.defaults());

                                execute(capture, dialect, initial, spec, reactive, channel);

                                assertEquals(1, subscriptions.get());
                                assertEquals(1, capture.executions);
                                List<Object> first = source.equals("merged")
                                        ? List.of(7L, "scope-A", "changed", "scope-A", "scope-old-name")
                                        : List.of(7L, "scope-A", "changed", "scope-A");
                                List<Object> second = source.equals("merged")
                                        ? List.of(8L, "scope-A", "changed", "scope-A", "scope-old-name")
                                        : List.of(8L, "scope-A", "changed", "scope-A");
                                assertEquals(List.of(first, second), capture.rows);
                                assertEquals(first.size(), capture.parameterCount);
                                assertEquals(first.size(), capture.parameterTypes.size());
                                assertTrue(capture.parameterTypes.subList(3, first.size()).stream()
                                        .allMatch(type -> type == String.class));
                                String sql = capture.sql.toLowerCase(Locale.ROOT);
                                assertTrue(sql.contains("case") || sql.contains("if("), sql);
                                assertFalse(sql.contains("scope-a"), sql);
                                assertFalse(sql.contains("scope-old-name"), sql);
                                if (expectedSql == null) expectedSql = capture.sql;
                                else assertEquals(expectedSql, capture.sql, "result/evidence/chunks share the plan");
                            }
                        }))));
    }

    @TestFactory
    Stream<DynamicTest> unscopedAndFieldOnlyUpsertsStillCompileAndDeliverTheirRows() {
        return dialects().flatMap(dialect -> Stream.of(false, true).flatMap(reactive ->
                Stream.of(false, true).map(fieldsOnly -> DynamicTest.dynamicTest(
                        dialect.name() + " reactive=" + reactive + " fields=" + fieldsOnly, () -> {
                            CapturingWriter capture = new CapturingWriter();
                            DataScope scope = fieldsOnly
                                    ? DataScope.none().withFields(FieldScope.writable("id", "org_id", "name"))
                                    : DataScope.none();
                            execute(capture, dialect, DataScope.none(),
                                    BatchSpec.upsert(form(), Flux.just(row(7L))).withScope(scope), reactive, "result");

                            assertEquals(1, capture.executions);
                            assertEquals(List.of(List.of(7L, "scope-A", "changed")), capture.rows);
                            assertEquals(3, capture.parameterCount);
                            String sql = capture.sql.toLowerCase(Locale.ROOT);
                            assertTrue(sql.contains("on conflict") || sql.contains("on duplicate key")
                                    || sql.startsWith("merge into"), sql);
                        }))));
    }

    @TestFactory
    Stream<DynamicTest> keyOnlyTargetScopeNeverCreatesABusinessUpdate() {
        DynamicForm keys = DynamicForm.builder("pure_keys", "pure_keys")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        return dialects().flatMap(dialect -> Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                dialect.name() + " reactive=" + reactive, () -> {
                    CapturingWriter capture = new CapturingWriter();
                    DataScope scope = DataScope.where(ConditionGroup.and().where("id", ">", 0L).build());
                    execute(capture, dialect, scope, BatchSpec.upsert(keys, Flux.just(Map.of("id", 7L))),
                            reactive, "result");

                    String sql = capture.sql.toLowerCase(Locale.ROOT);
                    if (dialect.name().equals("postgresql")) {
                        assertTrue(sql.contains("do nothing"), sql);
                        assertFalse(sql.contains("do update"), sql);
                    } else if (!dialect.name().equals("mysql")) {
                        assertFalse(sql.contains("when matched"), sql);
                        assertFalse(sql.contains("update set"), sql);
                    } else {
                        assertTrue(sql.endsWith("(select null union all select null))"), sql);
                    }
                    assertEquals(List.of(List.of(7L)), capture.rows, "no unused Scope binds for a no-op target");
                    assertEquals(1, capture.parameterCount);
                })));
    }

    private static void execute(CapturingWriter capture, RdbDialect dialect, DataScope defaults,
                                BatchSpec spec, boolean reactive, String channel) {
        execute(capture, dialect, defaults, spec, reactive, channel,
                com.flying.orm.core.scope.FieldUsePolicy.unrestricted());
    }

    private static void execute(CapturingWriter capture, RdbDialect dialect, DataScope defaults,
                                BatchSpec spec, boolean reactive, String channel,
                                com.flying.orm.core.scope.FieldUsePolicy policy) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), dialect);
        if (reactive) {
            ReactiveFormClient client = ReactiveFormClient.create(capture.reactive(), renderer)
                    .withDefaultDataScope(defaults).withFieldUsePolicy(policy);
            switch (channel) {
                case "result" -> client.writeBatch(spec).block();
                case "evidence" -> client.writeBatch(spec).block();
                default -> throw new AssertionError(channel);
            }
        } else {
            SyncFormClient client = SyncFormClient.create(unusedSqlExecutor(), capture.sync(), renderer)
                    .withDefaultDataScope(defaults).withFieldUsePolicy(policy);
            switch (channel) {
                case "result" -> client.writeBatch(spec);
                case "evidence" -> client.writeBatch(spec);
                default -> throw new AssertionError(channel);
            }
        }
    }

    private static Stream<RdbDialect> dialects() {
        return Stream.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.h2(),
                RdbDialect.oracle(), RdbDialect.sqlServer());
    }

    private static DataScope rowScope() {
        return DataScope.where(ConditionGroup.and().where("org_id", "=", "scope-A").build());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("scoped_rows", "scoped_rows")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("org_id", "VARCHAR"))
                .addField(DynamicField.of("name", "VARCHAR")).build();
    }

    private static Map<String, Object> row(long id) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("org_id", "scope-A");
        row.put("name", "changed");
        return row;
    }

    private static SyncSqlExecutor unusedSqlExecutor() {
        return (SyncSqlExecutor) Proxy.newProxyInstance(SyncSqlExecutor.class.getClassLoader(),
                new Class<?>[]{com.flying.orm.rdb.support.ListQueryFixture.class}, (proxy, method, args) -> {
                    throw new AssertionError("unexpected single SQL execution");
                });
    }

    private static final class CapturingWriter {
        private int executions;
        private String sql;
        private int parameterCount;
        private List<Class<?>> parameterTypes;
        private List<List<Object>> rows;

        private Mono<Void> accept(BatchWriteRequest request) {
            executions++;
            sql = request.sql();
            parameterCount = request.parameterCount();
            parameterTypes = request.parameterTypes();
            return Flux.from(request.rows()).map(Arrays::asList).collectList().doOnNext(values -> rows = values).then();
        }


        private BatchExecutionEvidence evidence(BatchWriteRequest request) {
            return com.flying.orm.rdb.repository.BatchEvidenceFixtures.successful(rows.size());
        }

        private SyncBatchExecutor sync() {
            return new SyncBatchExecutor() {
                @Override
                public BatchExecutionEvidence writeBatch(BatchWriteRequest request, java.util.function.LongConsumer rowCompleted) {
                    accept(request).block();
                    return evidence(request);
                }


            };
        }

        private ReactiveSqlExecutor reactive() {
            return new ReactiveSqlExecutor() {
                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.error(new AssertionError("unexpected query"));
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.error(new AssertionError("unexpected single write"));
                }

                @Override
                public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request, java.util.function.LongFunction<? extends org.reactivestreams.Publisher<Void>> rowCompleted) {
                    return accept(request).then(Mono.fromSupplier(() ->
                            evidence(request)));
                }


            };
        }
    }
}
