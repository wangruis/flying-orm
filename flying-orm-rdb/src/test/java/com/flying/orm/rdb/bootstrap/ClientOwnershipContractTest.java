package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.mapping.EntityFieldFiller;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaMigrationObservation;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.support.ListQueryFixture;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import com.flying.orm.rdb.template.SqlTemplateParameterProvider;
import com.flying.orm.rdb.template.SyncSqlTemplateParameterProvider;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClientOwnershipContractTest {
    @Test
    void assemblyAppliesDefaultsBeforePublishingBothKernels() {
        List<SqlExecutionOptions> applied = new ArrayList<>();
        List<SchemaMigrationObservation> observations = new ArrayList<>();
        SyncSqlExecutor jdbc = new ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { throw new AssertionError("unexpected query"); }
            public long rowsUpdated(SqlRequest request) { throw new AssertionError("missing DDL options"); }
            public long rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
                assertEquals("create table events (id bigint)", request.sql());
                applied.add(options);
                return 1L;
            }
        };
        ReactiveSqlExecutor reactive = new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) { throw new AssertionError("unexpected query"); }
            public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError("missing DDL options"); }
            public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
                return Mono.fromSupplier(() -> {
                    assertEquals("create table events (id bigint)", request.sql());
                    applied.add(options);
                    return 1L;
                });
            }
        };
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        BatchWriteOptions batch = BatchWriteOptions.of(37);
        SqlExecutionOptions ddl = SqlExecutionOptions.unlimited().withFetchSize(17);
        FlyingOrmAssemblyRequest request = new FlyingOrmAssemblyRequest(
                reactive, jdbc, (write, completed) -> { throw new AssertionError("unexpected batch"); },
                renderer, RdbDialect.postgresql(), StructuredConditionResolver.defaults(renderer.valueCodecs()),
                OrmCachePolicy.safeDefaults(), SqlExecutionOptions.unlimited(), IdGenerator.none(),
                EntityFieldFiller.none(), SqlTemplateRegistry.builder().build(), SqlTemplateParameterProvider.none(),
                SyncSqlTemplateParameterProvider.none(), ProtectedFieldRuntime.withoutKeys(), Map.of(), null, null,
                batch, ddl, observations::add);
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("id", "BIGINT")).build();
        SchemaMigrationPlan plan = new SchemaMigrationPlan(form.toTableDefinition(), false,
                List.of(new SqlRequest("create table events (id bigint)", List.of())), List.of());
        try (FlyingOrmClients clients = FlyingOrmClientAssembler.assemble(request)) {
            assertSame(batch, clients.syncForms().defaultBatchWriteOptions());
            assertSame(batch, clients.forms().defaultBatchWriteOptions());
            assertEquals(1L, clients.syncSchema().execute(plan).rowsUpdated());
            Mono<?> execution = clients.schema().execute(plan, clients.metadata());
            assertEquals(1, applied.size());
            execution.block(Duration.ofSeconds(2));
            assertEquals(List.of(ddl, ddl), applied);
            assertEquals(2, observations.size());
            assertEquals(List.of(1L, 1L), observations.stream().map(SchemaMigrationObservation::rowsUpdated).toList());
            assertOwnership(clients, clients);
        }
    }

    private static final JdbcConnectionAccess JDBC = JdbcConnectionAccess.of(request -> {
        throw new AssertionError("derivation must not acquire JDBC connections");
    }, (connection, request) -> {
        throw new AssertionError("derivation must not release JDBC connections");
    });
    private static final R2dbcConnectionAccess R2DBC = R2dbcConnectionAccess.of(request -> {
        throw new AssertionError("derivation must not acquire R2DBC connections");
    }, (signal, connection, request) -> {
        throw new AssertionError("derivation must not release R2DBC connections");
    });

    @TestFactory
    Stream<DynamicTest> derivedViewsUseTheOperatorsConfiguredComponents() {
        return IntStream.rangeClosed(1, 3).boxed().flatMap(kernels ->
                IntStream.range(0, 4).mapToObj(configuration -> DynamicTest.dynamicTest(
                        "kernels=" + kernels + ",configuration=" + configuration, () -> {
                    try (FlyingOrmClients original = clients(kernels);
                         FlyingOrmClients derived = switch (configuration) {
                             case 0 -> original.withDefaultDataScope(DataScope.tenant("tenant_id", "acme"));
                             case 1 -> original.withDefaultBatchWriteOptions(BatchWriteOptions.of(37));
                             case 2 -> original.withDefaultSchemaMigrationExecutionOptions(
                                     SqlExecutionOptions.unlimited().withFetchSize(17));
                             default -> original.withSchemaMigrationObserver(observation -> { });
                         }) {
                        assertOwnership(original, derived);
                    }
                })));
    }

    private static FlyingOrmClients clients(int kernels) {
        FlyingOrmClientBuilder builder = switch (kernels) {
            case 1 -> FlyingOrmClients.builder(JDBC);
            case 2 -> FlyingOrmClients.builder(R2DBC);
            default -> FlyingOrmClients.builder(JDBC, R2DBC);
        };
        return builder.dialect(RdbDialect.postgresql()).build();
    }

    @Test
    void chainedViewsShareScopedWriteSemanticsAndSurviveClosingTheBase() {
        List<SqlRequest> jdbcRequests = new ArrayList<>();
        List<SqlRequest> reactiveRequests = new ArrayList<>();
        IllegalStateException noDatabase = new IllegalStateException("captured request");
        JdbcConnectionAccess jdbc = JdbcConnectionAccess.of(request -> {
            jdbcRequests.add(request);
            throw noDatabase;
        }, (connection, request) -> { throw new AssertionError("no connection acquired"); });
        R2dbcConnectionAccess reactive = R2dbcConnectionAccess.of(request -> Mono.defer(() -> {
            reactiveRequests.add(request);
            return Mono.error(noDatabase);
        }), (signal, connection, request) -> { throw new AssertionError("no connection acquired"); });
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("tenant_id", "VARCHAR"))
                .addField(DynamicField.of("status", "VARCHAR")).build();
        WriteSpec write = WriteSpec.update(form, Map.of("status", "ready"),
                ConditionGroup.and().where("id", "=", 7L).build());
        BatchWriteOptions batch = BatchWriteOptions.of(37);
        try (FlyingOrmClients original = FlyingOrmClients.builder(jdbc, reactive).dialect(RdbDialect.postgresql()).build();
             FlyingOrmClients scoped = original.withDefaultDataScope(DataScope.tenant("tenant_id", "acme"));
             FlyingOrmClients derived = scoped.withDefaultBatchWriteOptions(batch)) {
            assertThrows(RuntimeException.class, () -> original.syncForms().update(write));
            assertThrows(RuntimeException.class,
                    () -> original.forms().update(write).block(Duration.ofSeconds(2)));
            assertEquals(List.of("ready", 7L), jdbcRequests.getFirst().parameters());
            assertEquals(List.of("ready", 7L), reactiveRequests.getFirst().parameters());
            original.close();
            scoped.close();
            assertOwnership(derived, derived);
            assertEquals(batch, derived.forms().defaultBatchWriteOptions());
            assertEquals(batch, derived.syncForms().defaultBatchWriteOptions());
            assertThrows(RuntimeException.class, () -> derived.syncForms().update(write));
            assertThrows(RuntimeException.class, () -> derived.syncOperator().dml().update(form)
                    .set("status", "ready").where("id", 7L).execute());
            Mono<Long> reactiveWrite = derived.forms().update(write);
            assertEquals(1, reactiveRequests.size());
            assertThrows(RuntimeException.class, () -> reactiveWrite.block(Duration.ofSeconds(2)));
            assertThrows(RuntimeException.class, () -> derived.operator().dml().update(form)
                    .set("status", "ready").where("id", 7L).execute().block(Duration.ofSeconds(2)));
            assertScopedRequests(jdbcRequests);
            assertScopedRequests(reactiveRequests);
        }
    }

    private static void assertScopedRequests(List<SqlRequest> requests) {
        assertEquals(3, requests.size());
        assertEquals(requests.get(1).sql(), requests.get(2).sql());
        assertEquals(List.of("ready", 7L, "acme"), requests.get(1).parameters());
        assertEquals(requests.get(1).parameters(), requests.get(2).parameters());
    }

    private static void assertOwnership(FlyingOrmClients original, FlyingOrmClients derived) {
        if (derived.jdbcAvailable()) {
            assertSame(derived.syncForms(), derived.syncOperator().forms());
            assertSame(derived.syncSchema(), derived.syncOperator().schema());
            assertSame(original.syncExecutor(), derived.syncExecutor());
            assertSame(original.syncForms().entityModels(), derived.syncForms().entityModels());
            assertSame(original.syncForms().entityModels().metadata(Event.class),
                    derived.syncForms().entityModels().metadata(Event.class));
        }
        if (derived.reactiveAvailable()) {
            assertSame(derived.forms(), derived.operator().forms());
            assertSame(derived.schema(), derived.operator().schema());
            assertSame(original.executor(), derived.executor());
            assertSame(original.metadata(), derived.metadata());
            assertSame(original.forms().entityModels(), derived.forms().entityModels());
            assertSame(original.forms().entityModels().metadata(Event.class),
                    derived.forms().entityModels().metadata(Event.class));
        }
    }

    @TableName("events")
    private record Event(@TableId Long id, String status) { }
}
