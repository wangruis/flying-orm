package com.flying.orm.rdb.operator;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.schema.SchemaMigrationExecutionOptions;
import com.flying.orm.rdb.schema.SchemaMigrationObserver;
import com.flying.orm.rdb.template.SqlTemplate;
import com.flying.orm.rdb.template.SqlTemplateEngine;
import com.flying.orm.rdb.template.SqlTemplateParameterProvider;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OperatorTemplatePlanReuseTest {

    private static final int TEMPLATE_COUNT = 4;
    private static final Map<String, Object> FIRST_VALUES =
            Map.of("key", "enabled", "id", 11, "tenant", 7);
    private static final Map<String, Object> NEXT_VALUES =
            Map.of("key", "disabled", "id", 22L, "tenant", 9L);

    @Test
    void scopeDerivationReusesBothBackendTemplatePlans() throws Exception {
        assertDerivationReusesPlans(clients -> clients.withDefaultDataScope(DataScope.tenant("tenant_id", 7)));
    }

    @Test
    void batchOptionDerivationReusesBothBackendTemplatePlans() throws Exception {
        assertDerivationReusesPlans(clients -> clients.withDefaultBatchWriteOptions(BatchWriteOptions.atomic(32)));
    }

    @Test
    void schemaObserverDerivationReusesBothBackendTemplatePlans() throws Exception {
        assertDerivationReusesPlans(clients -> clients.withSchemaMigrationObserver(SchemaMigrationObserver.noop()));
    }

    @Test
    void schemaExecutionOptionDerivationReusesBothBackendTemplatePlans() throws Exception {
        assertDerivationReusesPlans(clients -> clients.withDefaultSchemaMigrationExecutionOptions(
                SchemaMigrationExecutionOptions.defaults().withTimeout(Duration.ofSeconds(9))));
    }

    @Test
    void changedRegistryBuildsNewPlansWithoutChangingTheOriginalRegistry() throws Exception {
        NoConnections connections = new NoConnections();
        SqlTemplateRegistry originalRegistry = registry("events");
        SqlTemplateRegistry changedRegistry = registry("archived_events");
        try (FlyingOrmClients original = clients(connections, originalRegistry);
             FlyingOrmClients changed = clients(connections, changedRegistry)) {
            SqlTemplateEngine originalReactive = reactiveEngine(original);
            SqlTemplateEngine originalJdbc = jdbcEngine(original);
            SqlTemplateEngine changedReactive = reactiveEngine(changed);
            SqlTemplateEngine changedJdbc = jdbcEngine(changed);
            assertNotSame(originalReactive, changedReactive);
            assertNotSame(originalJdbc, changedJdbc);
            assertNotSame(originalReactive.render("lookup0", FIRST_VALUES, Map.of()).statement(),
                          changedReactive.render("lookup0", FIRST_VALUES, Map.of()).statement());
            assertNotSame(originalJdbc.render("lookup0", FIRST_VALUES, Map.of()).statement(),
                          changedJdbc.render("lookup0", FIRST_VALUES, Map.of()).statement());
            assertRenderedRequests(changedReactive, changedJdbc, "archived_events");

            DatabaseOperator reconfigured = original.operator().withSqlTemplates(
                    changedRegistry, SqlTemplateParameterProvider.none());
            SqlTemplateEngine reconfiguredEngine = engine(reconfigured, "sqlTemplateEngine");
            assertNotSame(originalReactive, reconfiguredEngine);
            assertEquals(reactiveSql("archived_events"),
                         reconfiguredEngine.render("lookup0", FIRST_VALUES, Map.of()).sql());
            assertRenderedRequests(originalReactive, originalJdbc, "events");
        }
        assertEquals(0, connections.totalRequests());
    }

    @Test
    void derivedTemplateCallsReadTrustedParametersForEachSubscriptionAndSyncCall() throws Exception {
        NoConnections connections = new NoConnections();
        AtomicInteger reactiveReads = new AtomicInteger();
        AtomicInteger jdbcReads = new AtomicInteger();
        try (FlyingOrmClients original = FlyingOrmClients.builder(connections.dataSource(), connections.factory())
                .configuredDialect("postgresql")
                .sqlTemplates(registry("events"))
                .sqlTemplateParameterProvider((id, parameters) -> {
                    assertEquals("lookup0", id);
                    assertEquals(Set.of("tenant"), parameters);
                    return Mono.just(Map.of("tenant", reactiveReads.incrementAndGet()));
                })
                .syncSqlTemplateParameterProvider((id, parameters) -> {
                    assertEquals("lookup0", id);
                    assertEquals(Set.of("tenant"), parameters);
                    return Map.of("tenant", jdbcReads.incrementAndGet());
                }).build();
             FlyingOrmClients derived = original.withDefaultDataScope(DataScope.tenant("tenant_id", 7))) {
            Flux<?> query = derived.operator().sqlTemplate("lookup0")
                    .bind("key", "enabled").bind("id", 11).query();
            assertEquals(0, reactiveReads.get());
            assertThrows(NoDatabaseAccess.class, () -> query.collectList().block(Duration.ofSeconds(2)));
            assertThrows(NoDatabaseAccess.class, () -> query.collectList().block(Duration.ofSeconds(2)));
            assertEquals(2, reactiveReads.get());
            assertEquals(2, connections.reactiveRequests.get());

            SyncSqlTemplateOperator syncQuery = derived.syncOperator().sqlTemplate("lookup0")
                    .bind("key", "enabled").bind("id", 11);
            assertEquals(0, jdbcReads.get());
            assertThrows(NoDatabaseAccess.class, syncQuery::query);
            assertThrows(NoDatabaseAccess.class, syncQuery::query);
            assertEquals(2, jdbcReads.get());
            assertEquals(2, connections.jdbcRequests.get());

            assertThrows(IllegalArgumentException.class,
                         () -> derived.operator().sqlTemplate("lookup0").bind("tenant", 99));
            assertThrows(IllegalArgumentException.class,
                         () -> derived.syncOperator().sqlTemplate("lookup0").bind("tenant", 99));
        }
    }

    @Test
    void closingBaseKeepsDerivedClientUsableUntilItsOwnClose() throws Exception {
        NoConnections connections = new NoConnections();
        FlyingOrmClients original = clients(connections, registry("events"));
        try (original;
             FlyingOrmClients derived = original.withDefaultDataScope(DataScope.tenant("tenant_id", 7))) {
            SqlTemplateEngine reactive = reactiveEngine(derived);
            SqlTemplateEngine jdbc = jdbcEngine(derived);
            original.close();
            assertThrows(IllegalStateException.class, original::operator);
            assertSame(reactive, reactiveEngine(derived));
            assertSame(jdbc, jdbcEngine(derived));
            assertRenderedRequests(reactive, jdbc, "events");
            derived.close();
            assertThrows(IllegalStateException.class, derived::operator);
            assertThrows(IllegalStateException.class, derived::syncOperator);
        }
        assertEquals(0, connections.totalRequests());
    }

    private static void assertDerivationReusesPlans(UnaryOperator<FlyingOrmClients> derivation) throws Exception {
        NoConnections connections = new NoConnections();
        try (FlyingOrmClients original = clients(connections, registry("events"));
             FlyingOrmClients derived = derivation.apply(original)) {
            SqlTemplateEngine originalReactive = reactiveEngine(original);
            SqlTemplateEngine originalJdbc = jdbcEngine(original);
            SqlTemplateEngine derivedReactive = reactiveEngine(derived);
            SqlTemplateEngine derivedJdbc = jdbcEngine(derived);
            assertAll(
                    () -> assertSame(originalReactive, derivedReactive, "R2DBC must reuse its compiled engine"),
                    () -> assertSame(originalJdbc, derivedJdbc, "JDBC must reuse its compiled engine"),
                    () -> assertNotSame(derivedReactive, derivedJdbc, "backend marker contexts stay separate"),
                    () -> assertRenderedRequests(derivedReactive, derivedJdbc, "events"),
                    () -> {
                        for (int index = 0; index < TEMPLATE_COUNT; index++) {
                            String templateId = "lookup" + index;
                            assertSame(originalReactive.render(templateId, FIRST_VALUES, Map.of()).statement(),
                                       derivedReactive.render(templateId, NEXT_VALUES, Map.of()).statement());
                            assertSame(originalJdbc.render(templateId, FIRST_VALUES, Map.of()).statement(),
                                       derivedJdbc.render(templateId, NEXT_VALUES, Map.of()).statement());
                        }
                    });
        }
        assertEquals(0, connections.totalRequests());
    }

    private static void assertRenderedRequests(SqlTemplateEngine reactive,
                                                SqlTemplateEngine jdbc,
                                                String table) {
        SqlRequest first = reactive.render("lookup0", FIRST_VALUES, Map.of());
        SqlRequest next = reactive.render("lookup0", NEXT_VALUES, Map.of());
        SqlRequest syncFirst = jdbc.render("lookup0", FIRST_VALUES, Map.of());
        SqlRequest syncNext = jdbc.render("lookup0", NEXT_VALUES, Map.of());
        assertEquals(reactiveSql(table), first.sql());
        assertEquals("select payload ?? ? from " + table + " where id = ? and tenant_id = ?", syncFirst.sql());
        assertSame(first.statement(), next.statement());
        assertSame(syncFirst.statement(), syncNext.statement());
        assertEquals(List.of("enabled", 11, 7), first.parameters());
        assertEquals(List.of("disabled", 22L, 9L), next.parameters());
        assertEquals(first.parameters(), syncFirst.parameters());
        assertEquals(next.parameters(), syncNext.parameters());
        assertEquals(Integer.class, first.parameters().get(1).getClass());
        assertEquals(Long.class, next.parameters().get(1).getClass());
    }

    private static String reactiveSql(String table) {
        return "select payload ? $1 from " + table + " where id = $2 and tenant_id = $3";
    }

    private static FlyingOrmClients clients(NoConnections connections, SqlTemplateRegistry registry) {
        return FlyingOrmClients.builder(connections.dataSource(), connections.factory())
                .configuredDialect("postgresql").sqlTemplates(registry).build();
    }

    private static SqlTemplateRegistry registry(String table) {
        SqlTemplateRegistry.Builder registry = SqlTemplateRegistry.builder();
        for (int index = 0; index < TEMPLATE_COUNT; index++) {
            registry.register(SqlTemplate.query("lookup" + index,
                    "select payload ? :key from " + table + " where id = :id and tenant_id = :tenant",
                    Set.of()), Set.of("tenant"));
        }
        return registry.build();
    }

    private static SqlTemplateEngine reactiveEngine(FlyingOrmClients clients) throws Exception {
        return engine(clients.operator(), "sqlTemplateEngine");
    }

    private static SqlTemplateEngine jdbcEngine(FlyingOrmClients clients) throws Exception {
        return engine(clients.syncOperator(), "templateEngine");
    }

    /** Observation only: no field mutation and no production hook. */
    private static SqlTemplateEngine engine(Object operator, String fieldName) throws Exception {
        Field field = operator.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (SqlTemplateEngine) field.get(operator);
    }

    private static final class NoDatabaseAccess extends RuntimeException {
        private NoDatabaseAccess() {
            super("database access deliberately stopped at the connection boundary");
        }
    }

    /** Only rejects acquisition; it never supplies a fake Connection, Result, commit or rollback. */
    private static final class NoConnections {
        private final AtomicInteger reactiveRequests = new AtomicInteger();
        private final AtomicInteger jdbcRequests = new AtomicInteger();

        private int totalRequests() {
            return reactiveRequests.get() + jdbcRequests.get();
        }

        private DataSource dataSource() {
            return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                    new Class<?>[]{DataSource.class}, (proxy, method, arguments) -> switch (method.getName()) {
                        case "getConnection" -> {
                            jdbcRequests.incrementAndGet();
                            throw new NoDatabaseAccess();
                        }
                        case "toString" -> "NoConnectionsDataSource";
                        case "hashCode" -> System.identityHashCode(proxy);
                        case "equals" -> proxy == arguments[0];
                        default -> throw new AssertionError("unexpected DataSource method: " + method);
                    });
        }

        private ConnectionFactory factory() {
            return new ConnectionFactory() {
                @Override
                public ConnectionFactoryMetadata getMetadata() {
                    return () -> "PostgreSQL";
                }

                @Override
                public Publisher<? extends Connection> create() {
                    reactiveRequests.incrementAndGet();
                    return Mono.error(new NoDatabaseAccess());
                }
            };
        }
    }
}
