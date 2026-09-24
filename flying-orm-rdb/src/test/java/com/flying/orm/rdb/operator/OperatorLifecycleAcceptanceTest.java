package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.form.spec.BatchOperation;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import io.r2dbc.spi.ConnectionFactories;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.sql.Connection;
import java.time.Duration;
import java.util.AbstractList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real JDBC/R2DBC checks at the public fluent boundary, including terminal resource cleanup. */
class OperatorLifecycleAcceptanceTest {
    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final DynamicForm USERS = DynamicForm.builder("users", "users")
            .addField(DynamicField.primaryKey("id", "INTEGER"))
            .addField(DynamicField.of("name", "VARCHAR(128)"))
            .addField(DynamicField.of("version", "INTEGER")).build();
    private final AtomicInteger jdbcAcquired = new AtomicInteger();
    private final AtomicInteger jdbcReleased = new AtomicInteger();
    private final AtomicInteger reactiveAcquired = new AtomicInteger();
    private final LinkedBlockingQueue<SignalType> reactiveReleased = new LinkedBlockingQueue<>();
    private Connection keepAlive;
    private FlyingOrmClients clients;

    @BeforeEach
    void open() throws Exception {
        String database = "operator_lifecycle_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database + ";DATABASE_TO_LOWER=TRUE");
        keepAlive = source.getConnection();
        try (var statement = keepAlive.createStatement()) {
            statement.execute("create table users(id int primary key, name varchar(128), version int)");
        }
        var factory = ConnectionFactories.get("r2dbc:h2:mem:///" + database + "?options=DATABASE_TO_LOWER=TRUE");
        clients = FlyingOrmClients.builder(
                JdbcConnectionAccess.of(request -> {
                    jdbcAcquired.incrementAndGet();
                    return source.getConnection();
                }, (connection, request) -> {
                    connection.close();
                    jdbcReleased.incrementAndGet();
                }),
                R2dbcConnectionAccess.of(request -> Mono.defer(() -> {
                    reactiveAcquired.incrementAndGet();
                    return Mono.from(factory.create());
                }), (signal, connection, request) -> Mono.from(connection.close())
                        .doOnSuccess(ignored -> reactiveReleased.add(signal))))
                .configuredDialect("h2").batchWriteOptions(BatchWriteOptions.of(1)).build();
    }

    @AfterEach
    void close() throws Exception {
        if (clients != null) { clients.close(); }
        if (keepAlive != null) { keepAlive.close(); }
    }

    @Test
    void reactiveQueryFreezesBuilderAndOwnsAConnectionPerSubscription() {
        clients.syncOperator().dml().insertBatch(USERS, List.of(row(1, "Alice"), row(2, "Bob")));
        var query = clients.operator().dml().query(USERS).where("id", 1);
        var pending = query.fetchMap();
        query.where("id", 2);
        assertEquals(0, reactiveAcquired.get());
        for (int i = 0; i < 2; i++) {
            assertEquals("Alice", pending.single().block(WAIT).get("name"));
        }
        assertEquals(2, reactiveAcquired.get());
        assertEquals(List.of(SignalType.ON_COMPLETE, SignalType.ON_COMPLETE), List.copyOf(reactiveReleased));
    }

    @Test
    void typedTerminalsFreezeBuilderAndStayColdUntilEachSubscription() {
        clients.syncOperator().dml().insertBatch(USERS, List.of(row(1, "Alice"), row(2, "Bob")));
        QueryOperator query = clients.operator().dml().query(USERS).where("id", 1);
        List<Mono<UserRow>> reads = List.of(
                query.one(UserRow.class),
                query.page(1, 10, UserRow.class).map(page -> page.rows().getFirst()),
                query.cursorPage(CursorPageQuery.first(10, CursorSort.asc("id")), UserRow.class)
                        .map(page -> page.rows().getFirst()),
                query.keysetPage(KeysetPageQuery.first(10, KeysetSort.asc("id", NullOrder.LAST)), UserRow.class)
                        .map(page -> page.rows().getFirst()));
        query.where("id", 2);
        assertEquals(0, reactiveAcquired.get());
        for (Mono<UserRow> pending : reads) {
            assertEquals("Alice", pending.block(WAIT).name());
            assertEquals("Alice", pending.block(WAIT).name());
        }
        assertEquals(10, reactiveAcquired.get()); // 页码分页每次包含 count 与页数据两条查询。
        assertEquals(reactiveAcquired.get(), reactiveReleased.size());
    }

    @Test
    void cancelledQueryReleasesItsConnectionOnce() throws Exception {
        clients.syncOperator().dml().insertBatch(USERS, List.of(row(1, "Alice"), row(2, "Bob")));
        var rows = clients.operator().dml().query(USERS).orderByAsc("id").fetchMap();
        assertEquals(1, rows.take(1).single().block(WAIT).get("id"));
        assertEquals(SignalType.CANCEL, reactiveReleased.poll(10, TimeUnit.SECONDS));
        assertEquals(1, reactiveAcquired.get());
        assertEquals(0, reactiveReleased.size());
    }

    @Test
    void mapperFailurePreservesTheErrorAndReleasesItsConnection() throws Exception {
        clients.syncOperator().dml().insert(USERS, row(1, "Alice"));
        var failure = new IllegalStateException("application mapper failed");
        var rows = clients.operator().dml().query(USERS).fetch(row -> { throw failure; });
        assertSame(failure, assertThrows(IllegalStateException.class, () -> rows.blockLast(WAIT)));
        assertEquals(SignalType.CANCEL, reactiveReleased.poll(10, TimeUnit.SECONDS));
        assertEquals(1, reactiveAcquired.get());
        assertEquals(0, reactiveReleased.size());
    }

    @Test
    void reactiveWriteFreezesValuesAndConditionsBeforeSubscription() {
        clients.syncOperator().dml().insertBatch(USERS, List.of(row(1, "Alice"), row(2, "Bob")));
        var update = clients.operator().dml().update(USERS).set("name", "Changed").where("id", 1);
        var pending = update.execute();
        update.set("name", "Late change").where("id", 2);
        assertEquals(0, reactiveAcquired.get());
        assertEquals("Alice", clients.syncOperator().dml().query(USERS).where("id", 1).one().get("name"));
        assertEquals(1L, pending.block(WAIT));
        assertEquals(1L, pending.block(WAIT));
        assertEquals(2, reactiveAcquired.get());
        assertEquals(2, reactiveReleased.size());
        assertEquals("Changed", clients.syncOperator().dml().query(USERS).where("id", 1).one().get("name"));
        assertEquals("Bob", clients.syncOperator().dml().query(USERS).where("id", 2).one().get("name"));
    }

    @Test
    void reactiveBatchDoesNotConsumeInputBeforeSubscriptionAndReexecutesPerSubscription() {
        var consumed = new AtomicInteger();
        var rows = Flux.defer(() -> {
            consumed.incrementAndGet();
            return Flux.just(row(1, "Alice"));
        });
        var pending = clients.operator().dml().upsertBatch(USERS, rows);
        assertEquals(0, consumed.get());
        assertEquals(0, reactiveAcquired.get());
        assertEquals(1, pending.block(WAIT).successfulCount());
        assertEquals(1, pending.block(WAIT).successfulCount());
        assertEquals(2, consumed.get());
        assertEquals(2, reactiveAcquired.get());
        assertEquals(2, reactiveReleased.size());
        assertEquals(1, clients.syncOperator().dml().query(USERS).fetchMap().size());
    }

    @Test
    void derivedClientSharesScopeAcrossEntrypointsWithoutChangingTheOriginal() {
        clients.syncOperator().dml().insertBatch(USERS, List.of(row(1, "Alice"), row(2, "Bob")));
        var scope = DataScope.orgOnly("id", 1);
        var query = QuerySpec.of(USERS, ConditionGroup.and().build());
        assertEquals(1, clients.syncOperator().withDefaultDataScope(scope).dml().query(USERS).fetchMap().size());
        assertEquals(2, clients.syncForms().select(query).size());
        var scoped = clients.withDefaultDataScope(scope);
        assertAll(
                () -> assertEquals(1, scoped.syncOperator().dml().query(USERS).fetchMap().size()),
                () -> assertEquals(1, scoped.syncForms().select(query).size()),
                () -> assertEquals(1, scoped.syncRepository(UserRow.class).createQuery().fetch().size()),
                () -> assertEquals(1L, scoped.operator().dml().query(USERS).fetchMap().count().block(WAIT)),
                () -> assertEquals(1L, scoped.forms().select(query).count().block(WAIT)),
                () -> assertEquals(1L, scoped.repository(UserRow.class).createQuery().fetch().count().block(WAIT)),
                () -> assertEquals(2, clients.syncRepository(UserRow.class).createQuery().fetch().size()));
    }

    @com.flying.orm.core.annotation.TableName("users")
    public record UserRow(int id, String name, int version) { }

    @Test
    void syncInsertPreservesExecutedRowsWhenLaterInputFails() throws Exception {
        assertPartialBatch(BatchOperation.INSERT);
    }

    @Test
    void syncUpsertPreservesExecutedRowsWhenLaterInputFails() throws Exception {
        assertPartialBatch(BatchOperation.UPSERT);
    }

    @Test
    void syncUpdatePreservesExecutedRowsWhenLaterInputFails() throws Exception {
        assertPartialBatch(BatchOperation.UPDATE);
    }

    @Test
    void inputFailureDuringHeadPlanningDoesNotAcquireAConnection() {
        var failure = new IllegalStateException("list head failed");
        for (BatchOperation operation : BatchOperation.values()) {
            assertSame(failure, assertThrows(IllegalStateException.class,
                    () -> executeFailingBatch(operation, failure, 1)));
        }
        assertEquals(0, jdbcAcquired.get());
        assertEquals(0, jdbcReleased.get());
    }

    private void assertPartialBatch(BatchOperation operation) throws Exception {
        if (operation != BatchOperation.INSERT) {
            try (var statement = keepAlive.createStatement()) {
                statement.execute("insert into users values(1, 'Before', 0), (2, 'Before', 0), (3, 'Before', 0)");
            }
        }
        var failure = new IllegalStateException("list input failed");
        var thrown = assertThrows(BatchExecutionEvidenceException.class,
                () -> executeFailingBatch(operation, failure, 3));
        var evidence = thrown.evidence();
        assertAll(
                () -> assertSame(failure, thrown.getCause()),
                () -> assertEquals(BatchExecutionState.PARTIAL, evidence.state()),
                () -> assertTrue(evidence.successfulCount() > 0),
                () -> assertTrue(evidence.inputCount() >= evidence.successfulCount() && evidence.inputCount() <= 3),
                () -> assertEquals(LongStream.range(0, evidence.successfulCount()).boxed().toList(),
                        evidence.successfulOffsets().boxed().toList()),
                () -> assertEquals(0, evidence.failedOffsets().count()),
                () -> assertEquals(1, jdbcAcquired.get()),
                () -> assertEquals(1, jdbcReleased.get()));
        long changed = clients.syncOperator().dml().query(USERS).where("name", "Changed").fetchMap().size();
        assertEquals(changed, evidence.successfulCount());
        assertEquals(changed, evidence.affectedRows().value());
    }

    private BatchExecutionEvidence executeFailingBatch(BatchOperation operation, RuntimeException failure, int failAt) {
        var dml = clients.syncOperator().dml();
        return switch (operation) {
            case INSERT -> dml.insertBatch(USERS, failingList(i -> row(i + 1, "Changed"), failure, failAt));
            case UPSERT -> dml.upsertBatch(USERS, failingList(i -> row(i + 1, "Changed"), failure, failAt));
            case UPDATE -> dml.updateBatch(USERS, failingList(i -> new BatchOptimisticUpdate(
                    Map.of("name", "Changed"), ConditionGroup.and().where("id", "=", i + 1).build(),
                    OptimisticLockOptions.increment("version", 0)), failure, failAt));
        };
    }

    private static <T> List<T> failingList(IntFunction<T> row, RuntimeException failure, int failAt) {
        return new AbstractList<>() {
            @Override
            public T get(int index) {
                if (index < failAt) { return row.apply(index); }
                throw failure;
            }

            @Override
            public int size() { return failAt + 1; }
        };
    }

    private static Map<String, Object> row(int id, String name) {
        return Map.of("id", id, "name", name, "version", 0);
    }
}
