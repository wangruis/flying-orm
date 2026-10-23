package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.sql.expression.SqlExpressionFunctions;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.form.spec.BatchSpec;
import io.r2dbc.spi.ConnectionFactories;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class MixedBatchLayoutTest {
    private static final DynamicForm FORM = DynamicForm.builder("mixed_rows", "mixed_rows")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("note", "VARCHAR")).build();

    @org.junit.jupiter.api.Test
    void fixedStatementCustomExecutorNeverReceivesValuesForAnotherColumn() {
        List<Object> boundNotes = new ArrayList<>();
        com.flying.orm.rdb.reactive.ReactiveSqlExecutor fixed = new com.flying.orm.rdb.reactive.ReactiveSqlExecutor() {
            public Flux<com.flying.orm.rdb.result.DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
                return Flux.empty();
            }
            public reactor.core.publisher.Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
                return reactor.core.publisher.Mono.just(0L);
            }
            public reactor.core.publisher.Mono<BatchExecutionEvidence> writeBatch(
                    com.flying.orm.rdb.batch.BatchWriteRequest request,
                    java.util.function.LongFunction<? extends org.reactivestreams.Publisher<Void>> completed) {
                return Flux.from(request.rows()).doOnNext(row -> boundNotes.add(row[1])).then(reactor.core.publisher.Mono.just(
                        new BatchExecutionEvidence.Accumulator().snapshot(BatchExecutionState.SUCCESS, null)));
            }
        };
        DynamicForm form = DynamicForm.builder("mixed_rows", "mixed_rows")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("note", "VARCHAR"))
                .addField(DynamicField.of("email", "VARCHAR")).build();
        ReactiveFormClient client = ReactiveFormClient.create(fixed, FormDataSqlRenderer.create(
                com.flying.orm.core.sql.render.SqlRenderer.builder().addDefaultTerms().build(),
                com.flying.orm.rdb.dialect.RdbDialect.postgresql()));
        Map<String, Object> first = new java.util.LinkedHashMap<>();
        first.put("id", 1L);
        first.put("note", "first");
        Map<String, Object> second = new java.util.LinkedHashMap<>();
        second.put("id", 2L);
        second.put("email", "private@example.test");
        assertThrows(UnsupportedOperationException.class, () -> client.writeBatch(BatchSpec.insert(form,
                Flux.just(first, second))).block());
        assertEquals(List.of("first"), boundNotes);
    }

    @TestFactory
    Stream<DynamicTest> omittedColumnsKeepDefaultsAndInputOrder() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(upsert ->
                DynamicTest.dynamicTest("reactive=" + reactive + "/upsert=" + upsert, () -> {
                    try (Database database = new Database()) {
                        List<Map<String, Object>> rows = List.of(Map.of("id", 1L, "note", "first"),
                                Map.of("id", 2L, "note", "second"), Map.of("id", 3L),
                                Map.of("id", 4L, "note", "last"));
                        BatchSpec spec = upsert ? BatchSpec.upsert(FORM, Flux.fromIterable(rows))
                                : BatchSpec.insert(FORM, Flux.fromIterable(rows));
                        BatchExecutionEvidence evidence = database.write(spec, reactive);
                        assertEquals(BatchExecutionState.SUCCESS, evidence.state());
                        assertEquals(List.of(0L, 1L, 2L, 3L), evidence.successfulOffsets().boxed().toList());
                        assertEquals(List.of("first", "second", "default", "last"), database.notes());
                    }
                })));
    }

    @TestFactory
    Stream<DynamicTest> expressionsAndPlainValuesShareTheNativeBatchPipeline() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(upsert ->
                DynamicTest.dynamicTest("expression/reactive=" + reactive + "/upsert=" + upsert, () -> {
                    try (Database database = new Database()) {
                        List<Map<String, Object>> rows = List.of(Map.of("id", 1L, "note", "plain"),
                                Map.of("id", 2L, "note", ValueExpression.call("lower", ValueExpression.value("SECOND"))),
                                Map.of("id", 3L, "note", ValueExpression.value("third")),
                                Map.of("id", 4L, "note", "last"));
                        BatchSpec spec = upsert ? BatchSpec.upsert(FORM, Flux.fromIterable(rows))
                                : BatchSpec.insert(FORM, Flux.fromIterable(rows));
                        assertEquals(4, database.write(spec, reactive).successfulCount());
                        assertEquals(List.of("plain", "second", "third", "last"), database.notes());
                    }
                })));
    }

    @TestFactory
    Stream<DynamicTest> conditionalUpdateStagesDoNotEraseExistingValues() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("reactive=" + reactive, () -> {
            try (Database database = new Database(); Statement statement = database.connection.createStatement()) {
                statement.execute("insert into mixed_rows values (1, 'keep'), (2, 'old')");
                List<Row> rows = List.of(new Row(1L, null), new Row(2L, "changed"),
                        new Row(3L, null), new Row(4L, "last"));
                BatchExecutionEvidence evidence = reactive
                        ? database.clients.repository(Row.class).upsertBatch(rows).block()
                        : database.clients.syncRepository(Row.class).upsertBatch(rows);
                assertEquals(4, evidence.successfulCount());
                assertEquals(List.of("keep", "changed", "default", "last"), database.notes());
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> insertDefaultsOnlyUpdateWhenTheCallerProvidedAValue() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("defaults=" + reactive, () -> {
            try (Database database = new Database(); Statement statement = database.connection.createStatement()) {
                statement.execute("insert into mixed_rows values (1, 'keep'), (2, 'old')");
                List<DefaultRow> rows = List.of(new DefaultRow(1L, null), new DefaultRow(2L, "provided"),
                        new DefaultRow(3L, null));
                BatchExecutionEvidence evidence = reactive
                        ? database.clients.repository(DefaultRow.class).upsertBatch(rows).block()
                        : database.clients.syncRepository(DefaultRow.class).upsertBatch(rows);
                assertEquals(3, evidence.successfulCount());
                assertEquals(List.of("keep", "provided", "filled"), database.notes());
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> aLaterLayoutCannotIntroduceAnUnapprovedField() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("policy=" + reactive, () -> {
            try (Database database = new Database()) {
                com.flying.orm.core.scope.FieldUsePolicy policy = com.flying.orm.core.scope.FieldUsePolicy.builder()
                        .allow("id", com.flying.orm.core.scope.FieldUse.INSERT).build();
                BatchSpec spec = BatchSpec.insert(FORM, Flux.just(Map.<String, Object>of("id", 1L),
                        Map.<String, Object>of("id", 2L, "note", "denied")));
                assertThrows(RuntimeException.class, () -> {
                    if (reactive) database.clients.forms().withFieldUsePolicy(policy).writeBatch(spec).block();
                    else database.clients.syncForms().withFieldUsePolicy(policy).writeBatch(spec);
                });
                assertTrue(database.notes().isEmpty());
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> layoutSwitchKeepsEarlierExecutionEvidenceOnFailure() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("reactive=" + reactive, () -> {
            try (Database database = new Database()) {
                BatchSpec spec = BatchSpec.insert(FORM, Flux.just(Map.<String, Object>of("id", 1L),
                        Map.<String, Object>of("id", 2L), Map.<String, Object>of("id", 1L, "note", "duplicate"),
                        Map.<String, Object>of("id", 3L)));
                BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                        () -> database.write(spec, reactive));
                assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state());
                assertEquals(List.of(0L, 1L), failure.evidence().successfulOffsets().boxed().toList());
                assertEquals(List.of("default", "default"), database.notes());
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> generatedKeysAndLifecycleOffsetsSurviveInsertLayoutSwitches() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("generated=" + reactive, () -> {
            try (Database database = new Database(); Statement statement = database.connection.createStatement()) {
                statement.execute("create table mixed_ids (id bigint generated by default as identity primary key,"
                        + " name varchar(32), note varchar(64) default 'default')");
                List<GeneratedRow> rows = List.of(new GeneratedRow("first", null), new GeneratedRow("second", "supplied"),
                        new GeneratedRow("third", null));
                List<Long> completed = new ArrayList<>();
                BatchExecutionEvidence evidence = reactive
                        ? database.clients.repository(GeneratedRow.class).withListener(event -> {
                            if (event.phase() == com.flying.orm.rdb.lifecycle.EntityLifecyclePhase.POST_PERSIST)
                                completed.add(event.entity().id);
                            return reactor.core.publisher.Mono.empty();
                        }).insertBatch(rows).block()
                        : database.clients.syncRepository(GeneratedRow.class).withListener(event -> {
                            if (event.phase() == com.flying.orm.rdb.lifecycle.EntityLifecyclePhase.POST_PERSIST)
                                completed.add(event.entity().id);
                            return reactor.core.publisher.Mono.empty();
                        }).insertBatch(rows);
                assertEquals(3, evidence.successfulCount());
                assertEquals(List.of(1L, 2L, 3L), rows.stream().map(row -> row.id).toList());
                assertEquals(List.of(1L, 2L, 3L), completed);
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> identityOnlyEntitiesInsertAndSaveWithoutInventingParameterValues() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of("insert", "batch", "save").map(operation ->
                DynamicTest.dynamicTest("identity-only=" + reactive + "/" + operation, () -> {
                    try (Database database = new Database(); Statement statement = database.connection.createStatement()) {
                        statement.execute("create table identity_only (id bigint generated by default as identity primary key)");
                        List<IdentityOnly> rows = List.of(new IdentityOnly(), new IdentityOnly(), new IdentityOnly());
                        if (reactive) {
                            com.flying.orm.rdb.repository.ReactiveFormRepository<IdentityOnly> repository =
                                    database.clients.repository(IdentityOnly.class);
                            switch (operation) {
                                case "insert" -> { for (IdentityOnly row : rows) assertEquals(1L, repository.insert(row).block()); }
                                case "batch" -> assertEquals(3, repository.insertBatch(rows).block().successfulCount());
                                default -> assertEquals(3, repository.upsertBatch(rows).block().successfulCount());
                            }
                        } else {
                            com.flying.orm.rdb.repository.SyncFormRepository<IdentityOnly> repository =
                                    database.clients.syncRepository(IdentityOnly.class);
                            switch (operation) {
                                case "insert" -> { for (IdentityOnly row : rows) assertEquals(1L, repository.insert(row)); }
                                case "batch" -> assertEquals(3, repository.insertBatch(rows).successfulCount());
                                default -> assertEquals(3, repository.upsertBatch(rows).successfulCount());
                            }
                        }
                        rows.forEach(row -> assertNotNull(row.id));
                        assertEquals(3, rows.stream().map(row -> row.id).distinct().count());
                        try (ResultSet saved = statement.executeQuery("select count(*) from identity_only")) {
                            assertTrue(saved.next());
                            assertEquals(3, saved.getInt(1));
                        }
                    }
                })));
    }

    @TableName("identity_only")
    static final class IdentityOnly {
        @TableId(type = com.flying.orm.core.annotation.IdType.AUTO) private Long id;
    }

    @TestFactory
    Stream<DynamicTest> defaultOnlyFormBatchExecutesEveryZeroParameterRow() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("default-form=" + reactive, () -> {
            try (Database database = new Database(); Statement statement = database.connection.createStatement()) {
                statement.execute("create table identity_only (id bigint generated by default as identity primary key)");
                DynamicForm form = DynamicForm.builder("identity_only", "identity_only")
                        .addField(DynamicField.primaryKey("id", "BIGINT")).build();
                BatchExecutionEvidence result = database.write(BatchSpec.insert(form,
                        Flux.just(Map.of(), Map.of(), Map.of())), reactive);
                assertEquals(List.of(0L, 1L, 2L), result.successfulOffsets().boxed().toList());
                try (ResultSet saved = statement.executeQuery("select count(*) from identity_only")) {
                    assertTrue(saved.next());
                    assertEquals(3, saved.getInt(1));
                }
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> generatedIdentityUpsertKeepsMixedInputOrderAndOnlyAssignsMissingKeys() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(knownFirst ->
                DynamicTest.dynamicTest("save=" + reactive + "/known-first=" + knownFirst, () -> {
                    try (Database database = new Database(); Statement statement = database.connection.createStatement()) {
                        statement.execute("create table mixed_ids (id bigint generated by default as identity primary key,"
                                + " name varchar(32), note varchar(64) default 'default')");
                        statement.execute("insert into mixed_ids (id,name) values (100,'old')");
                        GeneratedRow existing = new GeneratedRow("updated", "provided");
                        existing.id = 100L;
                        GeneratedRow missing = new GeneratedRow("explicit", null);
                        missing.id = 200L;
                        GeneratedRow first = new GeneratedRow("first", null);
                        GeneratedRow last = new GeneratedRow("last", "last-note");
                        List<GeneratedRow> rows = knownFirst ? List.of(existing, first, missing, last)
                                : List.of(first, existing, missing, last);
                        List<String> completed = new ArrayList<>();
                        com.flying.orm.rdb.lifecycle.ReactiveEntityListener<GeneratedRow> listener = event -> {
                            if (event.phase() == com.flying.orm.rdb.lifecycle.EntityLifecyclePhase.POST_PERSIST) {
                                assertNotNull(event.entity().id);
                                completed.add(event.entity().name);
                            }
                            return reactor.core.publisher.Mono.empty();
                        };
                        BatchExecutionEvidence evidence = reactive
                                ? database.clients.repository(GeneratedRow.class).withListener(listener).upsertBatch(rows).block()
                                : database.clients.syncRepository(GeneratedRow.class).withListener(listener).upsertBatch(rows);
                        assertEquals(List.of(0L, 1L, 2L, 3L), evidence.successfulOffsets().boxed().toList());
                        assertEquals(rows.stream().map(row -> row.name).toList(), completed);
                        assertEquals(100L, existing.id);
                        assertEquals(200L, missing.id);
                        assertNotNull(first.id);
                        assertNotNull(last.id);
                        assertNotEquals(first.id, last.id);
                        try (ResultSet saved = statement.executeQuery("select name from mixed_ids order by id")) {
                            List<String> names = new ArrayList<>();
                            while (saved.next()) names.add(saved.getString(1));
                            assertEquals(List.of("first", "last", "updated", "explicit"), names);
                        }
                    }
                })));
    }

    @TestFactory
    Stream<DynamicTest> failedGeneratedIdentityRowKeepsEarlierAssignmentsAndGlobalOffsets() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("save-failure=" + reactive, () -> {
            try (Database database = new Database(); Statement statement = database.connection.createStatement()) {
                statement.execute("create table mixed_ids (id bigint generated by default as identity primary key,"
                        + " name varchar(32) unique, note varchar(64))");
                statement.execute("insert into mixed_ids (id,name) values (100,'old')");
                GeneratedRow first = new GeneratedRow("first", null);
                GeneratedRow existing = new GeneratedRow("updated", "provided");
                existing.id = 100L;
                GeneratedRow rejected = new GeneratedRow("first", null);
                GeneratedRow untouched = new GeneratedRow("last", null);
                List<GeneratedRow> rows = List.of(first, existing, rejected, untouched);
                List<String> completed = new ArrayList<>();
                com.flying.orm.rdb.lifecycle.ReactiveEntityListener<GeneratedRow> listener = event -> {
                    if (event.phase() == com.flying.orm.rdb.lifecycle.EntityLifecyclePhase.POST_PERSIST) {
                        completed.add(event.entity().name);
                    }
                    return reactor.core.publisher.Mono.empty();
                };
                BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class, () -> {
                    if (reactive) database.clients.repository(GeneratedRow.class).withListener(listener)
                            .upsertBatch(Flux.fromIterable(rows), BatchWriteOptions.of(1)).block();
                    else database.clients.syncRepository(GeneratedRow.class).withListener(listener)
                            .upsertBatch(Flux.fromIterable(rows), BatchWriteOptions.of(1));
                });
                assertEquals(List.of(0L, 1L), failure.evidence().successfulOffsets().boxed().toList());
                assertEquals(List.of("first", "updated"), completed);
                assertNotNull(first.id);
                assertEquals(100L, existing.id);
                assertEquals(null, rejected.id);
                assertEquals(null, untouched.id);
                try (ResultSet saved = statement.executeQuery("select count(*) from mixed_ids")) {
                    assertTrue(saved.next());
                    assertEquals(2, saved.getInt(1));
                }
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> updateLayoutsKeepRowOrderNullBindingsAndConditions() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("update=" + reactive, () -> {
            try (Database database = new Database(); Statement statement = database.connection.createStatement()) {
                statement.execute("insert into mixed_rows values (1, 'first'), (2, 'second'), (3, 'third')");
                List<BatchUpdate> rows = List.of(
                        new BatchUpdate(Map.of("note", "changed"), where(1L)),
                        new BatchUpdate(Map.of("id", 4L), where(1L)),
                        new BatchUpdate(java.util.Collections.singletonMap("note", null),
                                com.flying.orm.core.condition.ConditionGroup.and().where("id", "in", List.of(2L, 3L)).build()),
                        new BatchUpdate(Map.of("note", "last"), where(4L)));
                BatchExecutionEvidence evidence = database.write(BatchSpec.update(FORM, Flux.fromIterable(rows)), reactive);
                assertEquals(List.of(0L, 1L, 2L, 3L), evidence.successfulOffsets().boxed().toList());
                assertEquals(java.util.Arrays.asList(null, null, "last"), database.notes());
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> laterUpdateLayoutMustApproveSetAndFilterFields() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(filter ->
                DynamicTest.dynamicTest("update-policy=" + reactive + "/filter=" + filter, () -> {
                    try (Database database = new Database(); Statement statement = database.connection.createStatement()) {
                        statement.execute("insert into mixed_rows values (1, 'first'), (2, 'second')");
                        com.flying.orm.core.scope.FieldUsePolicy policy = com.flying.orm.core.scope.FieldUsePolicy.builder()
                                .allow("id", com.flying.orm.core.scope.FieldUse.FILTER)
                                .allow("note", com.flying.orm.core.scope.FieldUse.UPDATE).build();
                        BatchUpdate later = filter ? new BatchUpdate(Map.of("note", "denied"),
                                com.flying.orm.core.condition.ConditionGroup.and().where("note", "=", "second").build())
                                : new BatchUpdate(Map.of("id", 4L), where(2L));
                        BatchSpec spec = BatchSpec.update(FORM, Flux.just(
                                new BatchUpdate(Map.of("note", "allowed"), where(1L)), later))
                                .withOptions(BatchWriteOptions.of(10));
                        Throwable failure = assertThrows(RuntimeException.class, () -> {
                            if (reactive) database.clients.forms().withFieldUsePolicy(policy).writeBatch(spec).block();
                            else database.clients.syncForms().withFieldUsePolicy(policy).writeBatch(spec);
                        });
                        while (failure.getCause() != null) failure = failure.getCause();
                        assertInstanceOf(com.flying.orm.core.scope.ScopeAccessException.class, failure);
                        assertEquals(List.of("first", "second"), database.notes());
                    }
                })));
    }

    @TestFactory
    Stream<DynamicTest> optimisticFailureAfterLayoutSwitchKeepsGlobalSuccessOffsets() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("update-lock=" + reactive, () -> {
            try (Database database = new Database(); Statement statement = database.connection.createStatement()) {
                statement.execute("alter table mixed_rows add version bigint default 0");
                statement.execute("insert into mixed_rows (id, note) values (1, 'first'), (2, 'second')");
                DynamicForm form = DynamicForm.builder("mixed_rows", "mixed_rows")
                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                        .addField(DynamicField.of("note", "VARCHAR"))
                        .addField(DynamicField.of("version", "BIGINT")).build();
                com.flying.orm.rdb.lock.OptimisticLockOptions lock =
                        com.flying.orm.rdb.lock.OptimisticLockOptions.increment("version", 0L);
                BatchSpec spec = BatchSpec.update(form, Flux.just(
                        new BatchUpdate(Map.of("note", "changed"), where(1L), lock),
                        new BatchUpdate(Map.of("id", 4L), where(1L), lock),
                        new BatchUpdate(Map.of("note", "not-executed"), where(2L), lock)));
                BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                        () -> database.write(spec, reactive));
                assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state());
                assertEquals(List.of(0L), failure.evidence().successfulOffsets().boxed().toList());
                assertEquals(List.of("changed", "second"), database.notes());
            }
        }));
    }

    private static com.flying.orm.core.condition.ConditionGroup where(long id) {
        return com.flying.orm.core.condition.ConditionGroup.and().where("id", "=", id).build();
    }

    @TableName("mixed_ids")
    static final class GeneratedRow {
        @TableId(type = com.flying.orm.core.annotation.IdType.AUTO)
        private Long id;
        private String name;
        @TableField(insertStrategy = FieldStrategy.NOT_NULL)
        private String note;

        GeneratedRow(String name, String note) { this.name = name; this.note = note; }
    }

    @TableName("mixed_rows")
    record Row(@TableId Long id,
               @TableField(insertStrategy = FieldStrategy.NOT_NULL,
                       updateStrategy = FieldStrategy.NOT_NULL) String note) { }

    @TableName("mixed_rows")
    record DefaultRow(@TableId Long id,
                      @TableField(fill = com.flying.orm.core.annotation.FieldFill.INSERT_DEFAULT) String note) { }

    private static final class Database implements AutoCloseable {
        final Connection connection;
        final FlyingOrmClients clients;

        Database() throws Exception {
            String name = "mixed_" + UUID.randomUUID().toString().replace("-", "");
            JdbcDataSource source = new JdbcDataSource();
            source.setURL("jdbc:h2:mem:" + name + ";DATABASE_TO_LOWER=TRUE");
            source.setUser("sa");
            connection = source.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table mixed_rows (id bigint primary key, note varchar(64) default 'default')");
            }
            clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                    ConnectionAccessTestSupport.reactive(ConnectionFactories.get(
                            "r2dbc:h2:mem:///" + name + ";USER=sa;DATABASE_TO_LOWER=TRUE")))
                    .configuredDialect("h2").renderer(SqlRenderer.builder().addDefaultTerms()
                            .expressionFunctions(SqlExpressionFunctions.builder()
                                    .register("lower", "lower", LogicalType.TEXT, LogicalType.TEXT).build()).build())
                    .fieldFiller((entity, field, operation, value) -> "filled").build();
        }

        BatchExecutionEvidence write(BatchSpec spec, boolean reactive) {
            BatchSpec configured = spec.withOptions(BatchWriteOptions.of(10));
            return reactive ? clients.forms().writeBatch(configured).block() : clients.syncForms().writeBatch(configured);
        }

        List<String> notes() throws Exception {
            List<String> notes = new ArrayList<>();
            try (Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery("select note from mixed_rows order by id")) {
                while (rows.next()) notes.add(rows.getString(1));
            }
            return notes;
        }

        public void close() throws Exception {
            clients.close();
            connection.close();
        }
    }
}
