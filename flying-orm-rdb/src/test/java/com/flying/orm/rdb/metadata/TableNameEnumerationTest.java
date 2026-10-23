package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import static org.junit.jupiter.api.Assertions.*;

class TableNameEnumerationTest {

    @Test
    void cacheWrapperPreservesCustomCurrentSchemaEntryAndDefersIt() {
        AtomicInteger calls = new AtomicInteger();
        ReactiveFormMetadataReader custom = new ReactiveFormMetadataReader() {
            public Flux<String> readTableNames() {
                calls.incrementAndGet();
                return Flux.just("custom");
            }
            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                throw new AssertionError();
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
            }

        };
        Flux<String> names = ReactiveFormMetadataReaders.cached(custom).readTableNames();
        assertEquals(0, calls.get());
        assertEquals(List.of("custom"), names.collectList().block());
        assertEquals(List.of("custom"), names.collectList().block());
        assertEquals(2, calls.get());
    }

    @Test
    void builtInReadersShareBoundSchemaQueriesWithoutPerTableLoadsOrCaching() {
        String schema = "MiX'ed.schema_%";
        List<DynamicRow> names = List.of(row("A.B"), row("MiXeD"));
        for (RdbDialect dialect : dialects()) {
            List<SqlRequest> requests = new ArrayList<>();
            JdbcFormMetadataReader jdbc = JdbcFormMetadataReaders.cached(sync(request -> {
                requests.add(request);
                return names;
            }), dialect, CacheRegionPolicy.metadataDefaults(), MetadataCacheInvalidator.none());
            ReactiveFormMetadataReader reactive = ReactiveFormMetadataReaders.cached(
                    ReactiveFormMetadataReaders.create(reactive(request -> {
                        requests.add(request);
                        return Flux.fromIterable(names);
                    }), dialect));
            assertEquals(List.of("A.B", "MiXeD"), jdbc.readTableNames(schema));
            assertEquals(List.of("A.B", "MiXeD"), jdbc.readTableNames(schema));
            Flux<String> result = reactive.readTableNames(schema);
            assertEquals(2, requests.size(), "assembly must not query");
            assertEquals(List.of("A.B", "MiXeD"), result.collectList().block());
            assertEquals(List.of("A.B", "MiXeD"), result.collectList().block());
            assertEquals(4, requests.size(), "one fresh query per call/subscription, no per-table loads");
            for (SqlRequest request : requests) {
                assertEquals(requests.getFirst().sql(), request.sql());
                assertEquals(List.of(schema), request.parameters());
                assertFalse(request.sql().contains(schema));
                assertTrue(request.sql().contains("order by TABLE_NAME"));
            }
        }
    }

    @Test
    void omittedSchemaUsesDatabaseCurrentSchemaWithoutChangingConnectionState() {
        List<String> expressions = List.of("current_schema()", "database()", "current_schema()",
                "sys_context('USERENV', 'CURRENT_SCHEMA')", "schema_name()");
        List<RdbDialect> dialects = dialects();
        for (int i = 0; i < dialects.size(); i++) {
            List<SqlRequest> requests = new ArrayList<>();
            JdbcFormMetadataReader reader = JdbcFormMetadataReaders.create(sync(request -> {
                requests.add(request);
                return List.of();
            }), dialects.get(i));
            assertEquals(List.of(), reader.readTableNames());
            assertEquals(1, requests.size());
            assertEquals(List.of(), requests.getFirst().parameters());
            assertTrue(requests.getFirst().sql().contains(expressions.get(i)));
        }
    }

    @Test
    void reactiveNamesPreserveContextDemandAndCancellationThroughCacheWrapper() {
        AtomicInteger queries = new AtomicInteger();
        AtomicInteger emitted = new AtomicInteger();
        AtomicInteger cancelled = new AtomicInteger();
        ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(
                ReactiveFormMetadataReaders.create(reactive(request -> {
                    queries.incrementAndGet();
                    return Flux.deferContextual(context -> {
                        assertEquals("scope", context.get("table-names"));
                        return Flux.just(row("one"), row("two"))
                                .doOnNext(ignored -> emitted.incrementAndGet())
                                .doOnCancel(cancelled::incrementAndGet);
                    });
                }), RdbDialect.postgresql()));
        Flux<String> names = reader.readTableNames().contextWrite(context -> context.put("table-names", "scope"));
        assertEquals(0, queries.get());
        List<String> received = new ArrayList<>();
        BaseSubscriber<String> subscriber = new BaseSubscriber<>() {
            protected void hookOnSubscribe(Subscription subscription) { }
            protected void hookOnNext(String name) { received.add(name); }
        };
        names.subscribe(subscriber);
        assertEquals(1, queries.get());
        assertEquals(0, emitted.get());
        subscriber.request(1);
        assertEquals(List.of("one"), received);
        assertEquals(1, emitted.get());
        subscriber.cancel();
        assertEquals(1, cancelled.get());
    }

    @Test
    void emptySchemaAndDatabaseErrorsAreNotReplacedWithFallbackData() {
        IllegalStateException failure = new IllegalStateException("metadata unavailable");
        JdbcFormMetadataReader jdbc = JdbcFormMetadataReaders.create(sync(request -> {
            throw failure;
        }), RdbDialect.postgresql());
        ReactiveFormMetadataReader reactive = ReactiveFormMetadataReaders.cached(
                ReactiveFormMetadataReaders.create(reactive(request -> Flux.error(failure)),
                        RdbDialect.postgresql()));
        assertSame(failure, assertThrows(IllegalStateException.class, () -> jdbc.readTableNames("")));
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> reactive.readTableNames("").collectList().block()));
        assertEquals(List.of(), ReactiveFormMetadataReaders.create(reactive(request -> Flux.empty()),
                RdbDialect.postgresql()).readTableNames("missing").collectList().block());
    }

    @Test
    void h2ExecutionIncludesViewsKeepsLiteralNamesAndSeesSchemaChanges() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:");
             Statement statement = connection.createStatement()) {
            statement.execute("create table \"A.B\" (id int)");
            statement.execute("create view \"MiXeD\" as select id from \"A.B\"");
            statement.execute("create schema \"Other.Schema\"");
            statement.execute("create table \"Other.Schema\".\"only\" (id int)");
            JdbcFormMetadataReader reader = JdbcFormMetadataReaders.create(sync(request -> {
                try (PreparedStatement query = connection.prepareStatement(request.sql())) {
                    for (int i = 0; i < request.parameters().size(); i++) {
                        query.setObject(i + 1, request.parameters().get(i));
                    }
                    try (ResultSet rows = query.executeQuery()) {
                        List<DynamicRow> result = new ArrayList<>();
                        while (rows.next()) result.add(row(rows.getString(1)));
                        return result;
                    }
                } catch (SQLException error) {
                    throw new AssertionError(error);
                }
            }), RdbDialect.h2());
            assertEquals(List.of("A.B", "MiXeD"), reader.readTableNames());
            assertEquals(List.of("only"), reader.readTableNames("Other.Schema"));
            assertEquals(List.of(), reader.readTableNames("missing"));
            statement.execute("drop view \"MiXeD\"");
            assertEquals(List.of("A.B"), reader.readTableNames());
        }
    }

    private static List<RdbDialect> dialects() {
        return List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.h2(),
                RdbDialect.oracle(), RdbDialect.sqlServer());
    }

    private static DynamicRow row(String name) {
        return DynamicRow.copyOf(Map.of("TABLE_NAME", name));
    }

    private static SyncSqlExecutor sync(Function<SqlRequest, List<DynamicRow>> query) {
        return new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { return query.apply(request); }
            public long rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                throw new AssertionError("unexpected write");
            }
        };
    }

    private static ReactiveSqlExecutor reactive(Function<SqlRequest, Flux<DynamicRow>> query) {
        return (ReactiveSqlExecutor) Proxy.newProxyInstance(TableNameEnumerationTest.class.getClassLoader(),
                new Class<?>[]{ReactiveSqlExecutor.class}, (self, method, arguments) -> {
                    if (!method.getName().equals("query")) throw new AssertionError(method.getName());
                    return query.apply((SqlRequest) arguments[0]);
                });
    }
}
