package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrdinaryProtectedWriteBatchingTest {
    private static final class Counts {
        final int owners;
        final boolean invalidInsert;
        int ownerReads;
        int businessWrites;
        int deletes;
        int inserts;
        int begins;
        int commits;
        int rollbacks;
        int closes;
        final List<String> order = new ArrayList<>();

        Counts(int owners, boolean invalidInsert) {
            this.owners = owners;
            this.invalidInsert = invalidInsert;
        }
    }

    @TestFactory
    Stream<DynamicTest> batchesFiveHundredOwnersOnBothExecutors() {
        return bothExecutors("batches 500 owners", reactive -> {
            Counts counts = new Counts(500, false);
            SqlWriteResult result = execute(reactive, counts);

            assertEquals(500, result.affectedRows());
            assertEquals(List.of("D:500", "I:500"), counts.order);
            assertLifecycle(counts, 1, 0);
        });
    }

    @TestFactory
    Stream<DynamicTest> preservesTheBoundedDeleteBeforeInsertSegmentsOnBothExecutors() {
        return bothExecutors("preserves bounded delete before insert", reactive -> {
            Counts counts = new Counts(501, false);
            SqlWriteResult result = execute(reactive, counts);

            assertEquals(501, result.affectedRows());
            assertEquals(List.of("D:500", "I:500", "D:1", "I:1"), counts.order);
            assertLifecycle(counts, 1, 0);
        });
    }

    @TestFactory
    Stream<DynamicTest> invalidTokenCountsRollbackOnBothExecutors() {
        return bothExecutors("rolls back invalid token counts", reactive -> {
            Counts counts = new Counts(500, true);

            assertThrows(RuntimeException.class, () -> execute(reactive, counts));

            assertEquals(List.of("D:500", "I:500"), counts.order);
            assertLifecycle(counts, 0, 1);
        });
    }

    @TestFactory
    Stream<DynamicTest> batchesFiveHundredOwnersWithOnlyDeleteWorkWhenTokensAreEmpty() {
        return bothExecutors("batches 500 owners with empty tokens", reactive -> {
            Counts counts = new Counts(500, false);
            SqlWriteResult result = execute(reactive, counts, work(0));

            assertEquals(500, result.affectedRows());
            assertEquals(List.of("D:500"), counts.order);
            assertEquals(0, counts.inserts);
            assertLifecycle(counts, 1, 0);
        });
    }

    @TestFactory
    Stream<DynamicTest> batchesSingleInsertTokensAcrossFieldsOnBothExecutors() {
        return bothExecutors("batches one insert across protected fields", reactive -> {
            Counts counts = new Counts(1, false);

            SqlWriteResult result = execute(reactive, counts, insertWork());

            assertEquals(1, result.affectedRows());
            assertEquals(List.of("I:2"), counts.order);
            assertEquals(0, counts.ownerReads);
            assertEquals(1, counts.businessWrites);
            assertEquals(1, counts.inserts);
            assertEquals(1, counts.commits);
            assertEquals(0, counts.rollbacks);
            assertEquals(1, counts.closes);
        });
    }

    @TestFactory
    Stream<DynamicTest> reactiveProtectedUpdateSchedulesOnlyOneExecutionDeadline() {
        return Stream.of(Duration.ZERO, Duration.ofSeconds(30)).map(timeout ->
                DynamicTest.dynamicTest("execution deadline tasks [timeout=" + timeout + "]", () -> {
                    AtomicInteger scheduledTasks = new AtomicInteger();
                    String hook = getClass().getName() + ".executionDeadline";
                    Schedulers.onScheduleHook(hook, task -> {
                        scheduledTasks.incrementAndGet();
                        return task;
                    });
                    try {
                        Counts counts = new Counts(2, false);
                        // Exclude the separately owned cleanup deadline from this execution-only count.
                        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                                .withTimeout(timeout).withCleanupTimeout(Duration.ZERO);

                        SqlWriteResult result = execute(true, counts, work(1), options);

                        assertEquals(2, result.affectedRows());
                        assertLifecycle(counts, 1, 0);
                        assertEquals(timeout.isZero() ? 0 : 1, scheduledTasks.get(),
                                "the owner query must reuse the protected transaction deadline");
                    } finally {
                        Schedulers.resetOnScheduleHook(hook);
                    }
                }));
    }

    @TestFactory
    Stream<DynamicTest> reactiveProtectedUpdateKeepsOwnerRowBudgetWithOrWithoutDeadline() {
        return Stream.of(Duration.ZERO, Duration.ofSeconds(30)).map(timeout ->
                DynamicTest.dynamicTest("owner row budget [timeout=" + timeout + "]", () -> {
                    Counts counts = new Counts(2, false);
                    SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                            .withTimeout(timeout).withMaxRows(1).withCleanupTimeout(Duration.ZERO);

                    SqlRowLimitExceededException failure = assertThrows(SqlRowLimitExceededException.class,
                            () -> execute(true, counts, work(1), options));

                    assertEquals(1, failure.maxRows());
                    assertEquals(1, failure.overflowIndex());
                    assertEquals(1, counts.ownerReads);
                    assertEquals(0, counts.businessWrites);
                    assertEquals(0, counts.commits);
                    assertEquals(1, counts.rollbacks);
                    assertEquals(1, counts.closes);
                }));
    }

    private static Stream<DynamicTest> bothExecutors(String name,
                                                     Consumer<Boolean> test) {
        return Stream.of(false, true)
                .map(reactive -> DynamicTest.dynamicTest(
                        name + " [reactive=" + reactive + "]", () -> test.accept(reactive)));
    }

    @TestFactory
    Stream<DynamicTest> reactiveProtectedGeneratedKeyDeadlineKeepsTransactionTruth() {
        return Stream.of("external before write", "external after write", "owned rollback", "owned unknown")
                .map(mode -> DynamicTest.dynamicTest(mode, () -> assertProtectedKeyDeadline(mode)));
    }

    @TestFactory
    Stream<DynamicTest> completedGeneratedKeysDoNotRelabelLaterTokenFailures() {
        return Stream.of("external tokens", "owned tokens unknown")
                .map(mode -> DynamicTest.dynamicTest(mode, () -> assertProtectedKeyDeadline(mode)));
    }

    @TestFactory
    Stream<DynamicTest> generatedKeyResultDeliveryDeadlineKeepsTransactionTruth() {
        return Stream.of("external delivery", "owned delivery rollback", "owned delivery unknown")
                .map(mode -> DynamicTest.dynamicTest(mode, () -> assertProtectedKeyDeadline(mode)));
    }

    @SuppressWarnings("unchecked")
    private void assertProtectedKeyDeadline(String mode) {
        boolean external = mode.startsWith("external");
        boolean observed = !mode.equals("external before write");
        boolean rollbackUnknown = mode.endsWith("unknown");
        boolean deliveryPending = mode.contains("delivery");
        boolean keysCompleted = mode.contains("tokens") || deliveryPending;
        Counts counts = new Counts(1, false);
        IllegalStateException rollbackFailure = new IllegalStateException("rollback acknowledgement lost");
        IllegalStateException tokenFailure = new IllegalStateException("token write failed");
        AtomicInteger completedKeyResults = new AtomicInteger();
        AtomicReference<Object> tokenOwner = new AtomicReference<>();
        Result result = proxy(Result.class, (proxy, method, arguments) -> {
            if (!method.getName().equals("flatMap")) {
                throw new UnsupportedOperationException(method.getName());
            }
            Function<Result.Segment, Publisher<?>> mapper = (Function<Result.Segment, Publisher<?>>) arguments[0];
            if (keysCompleted) {
                return Flux.concat(Mono.<Result.Segment>just((Result.UpdateCount) () -> 1L),
                                Flux.from(r2dbcRows(1).map((row, metadata) -> (Result.RowSegment) () -> row)))
                        .concatMap(segment -> Flux.from(mapper.apply(segment)), 1)
                        .doOnComplete(completedKeyResults::incrementAndGet);
            }
            return observed ? Flux.from(mapper.apply((Result.UpdateCount) () -> 1L))
                    .thenMany(Flux.never()) : Flux.never();
        });
        Statement statement = proxy(Statement.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "execute" -> { counts.businessWrites++; yield Flux.just(result); }
            case "bind", "returnGeneratedValues" -> proxy;
            default -> throw new UnsupportedOperationException(method.getName());
        });
        Statement tokenStatement = proxy(Statement.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "bind" -> {
                if ((Integer) arguments[0] == 0) {
                    tokenOwner.set(arguments[1]);
                }
                yield proxy;
            }
            case "execute" -> { counts.inserts++; yield Flux.error(tokenFailure); }
            default -> throw new UnsupportedOperationException(method.getName());
        });
        Connection connection = proxy(Connection.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "createStatement" -> ((String) arguments[0]).startsWith("insert into token_index")
                    ? tokenStatement : statement;
            case "isAutoCommit" -> true;
            case "setAutoCommit" -> Mono.empty();
            case "beginTransaction" -> { counts.begins++; yield Mono.empty(); }
            case "commitTransaction" -> { counts.commits++; yield Mono.empty(); }
            case "rollbackTransaction" -> {
                counts.rollbacks++;
                yield rollbackUnknown ? Mono.error(rollbackFailure) : Mono.empty();
            }
            case "close" -> { counts.closes++; yield Mono.empty(); }
            default -> throw new UnsupportedOperationException(method.getName());
        });
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(new ConnectionFactory() {
            public Publisher<? extends Connection> create() { return Mono.just(connection); }
            public ConnectionFactoryMetadata getMetadata() { return () -> "PostgreSQL"; }
        });
        if (external) {
            executor = executor.withTransactionParticipant(() -> Mono.just(R2dbcTransactionContext.external(connection)));
        }
        ProtectedWriteWork work = new ProtectedWriteWork(ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into business_row(secret) values (?)", List.of("ciphertext")),
                null, List.of("id"), Map.of(), "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("secret", List.of(new byte[32]))));
        AtomicReference<Runnable> deadline = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<SqlWriteResult> pendingResult = new AtomicReference<>();
        AtomicInteger values = new AtomicInteger();
        String hook = getClass().getName() + ".protectedKeyEvidenceDeadline";
        Schedulers.onScheduleHook(hook, task -> {
            deadline.compareAndSet(null, task);
            return task;
        });
        if (deliveryPending) {
            Hooks.onEachOperator(hook, Operators.<Object, Object>lift((operator, actual) ->
                    new CoreSubscriber<Object>() {
                        public Context currentContext() { return actual.currentContext(); }
                        public void onSubscribe(Subscription subscription) { actual.onSubscribe(subscription); }
                        public void onNext(Object value) {
                            if (value instanceof SqlWriteResult result
                                    && pendingResult.compareAndSet(null, result)) {
                                assertNotNull(deadline.get());
                                deadline.get().run();
                            }
                            actual.onNext(value);
                        }
                        public void onError(Throwable failure) { actual.onError(failure); }
                        public void onComplete() { actual.onComplete(); }
                    }));
        }
        Disposable subscription = null;
        try {
            subscription = executor.atomicProtectedWrite(work, SqlExecutionOptions.safeDefaults()
                            .withTimeout(Duration.ofSeconds(30)).withCleanupTimeout(Duration.ZERO))
                    .subscribe(ignored -> values.incrementAndGet(), error::set);
            assertEquals(1, counts.businessWrites);
            if (deliveryPending) {
                assertEquals(1, completedKeyResults.get());
                assertNotNull(pendingResult.get());
                assertEquals(1L, pendingResult.get().affectedRows());
                assertEquals(1L, pendingResult.get().generatedKeys().getFirst().get("id"));
                assertEquals(0, counts.inserts, "the deadline fires before generated keys reach token work");
            } else if (keysCompleted) {
                assertEquals(1, completedKeyResults.get());
                assertEquals(1, counts.inserts, "the token publisher must run after keys are fully consumed");
                assertEquals(1L, tokenOwner.get(), "the decoded generated owner key must reach token binding");
            } else {
                assertNotNull(deadline.get());
                deadline.get().run();
            }

            Throwable cause = error.get();
            if ((!keysCompleted || deliveryPending) && observed && (external || rollbackUnknown)) {
                GeneratedKeyReadException failure = assertInstanceOf(GeneratedKeyReadException.class, cause);
                assertEquals(1L, failure.affectedRows());
                cause = failure.getCause();
            }
            if (rollbackUnknown) {
                assertEquals(RdbErrorKind.UNKNOWN, assertInstanceOf(RdbException.class, cause).kind());
                assertSame(rollbackFailure, cause.getCause());
                if (deliveryPending) {
                    assertEquals(1, cause.getSuppressed().length);
                    GeneratedKeyReadException keyFailure = assertInstanceOf(
                            GeneratedKeyReadException.class, cause.getSuppressed()[0]);
                    assertEquals(1L, keyFailure.affectedRows());
                    assertInstanceOf(SqlExecutionTimeoutException.class, keyFailure.getCause());
                } else if (keysCompleted) {
                    assertEquals(1, cause.getSuppressed().length);
                    assertSame(tokenFailure, cause.getSuppressed()[0]);
                }
            } else if (keysCompleted && !deliveryPending) {
                assertSame(tokenFailure, cause);
            } else {
                assertInstanceOf(SqlExecutionTimeoutException.class, cause);
            }
            assertEquals(0, values.get());
            assertEquals(external ? 0 : 1, counts.begins);
            assertEquals(0, counts.commits);
            assertEquals(external ? 0 : 1, counts.rollbacks);
            assertEquals(external ? 0 : 1, counts.closes);
        } finally {
            if (subscription != null) {
                subscription.dispose();
            }
            Hooks.resetOnEachOperator(hook);
            Schedulers.resetOnScheduleHook(hook);
        }
    }

    private static void assertLifecycle(Counts counts, int commits, int rollbacks) {
        assertEquals(1, counts.ownerReads);
        assertEquals(1, counts.businessWrites);
        assertEquals(commits, counts.commits);
        assertEquals(rollbacks, counts.rollbacks);
        assertEquals(1, counts.closes);
    }

    private static SqlWriteResult execute(boolean reactive, Counts counts) {
        return execute(reactive, counts, work(1));
    }

    private static SqlWriteResult execute(boolean reactive,
                                          Counts counts,
                                          ProtectedWriteWork work) {
        return execute(reactive, counts, work, SqlExecutionOptions.safeDefaults());
    }

    private static SqlWriteResult execute(boolean reactive,
                                          Counts counts,
                                          ProtectedWriteWork work,
                                          SqlExecutionOptions options) {
        if (!reactive) {
            java.sql.Connection connection = jdbcConnection(counts);
            DataSource dataSource = proxy(DataSource.class, (proxy, method, arguments) -> switch (method.getName()) {
                case "getConnection" -> connection;
                default -> defaultValue(method.getReturnType());
            });
            return JdbcSqlExecutor.create(dataSource)
                    .atomicProtectedWrite(work, options);
        }
        Connection connection = r2dbcConnection(counts);
        ConnectionFactory factory = new ConnectionFactory() {
            public Publisher<? extends Connection> create() { return Mono.just(connection); }
            public ConnectionFactoryMetadata getMetadata() { return () -> "PostgreSQL"; }
        };
        return R2dbcSqlExecutor.create(factory).atomicProtectedWrite(work, options)
                .block(Duration.ofSeconds(5));
    }

    private static ProtectedWriteWork work(int tokenCount) {
        List<byte[]> tokens = new ArrayList<>(tokenCount);
        for (int index = 0; index < tokenCount; index++) {
            tokens.add(new byte[32]);
        }
        return new ProtectedWriteWork(ProtectedWriteWork.Kind.UPDATE,
                new SqlRequest("update business_row set secret = ? where group_id = ?", List.of("ciphertext", 7L)),
                new SqlRequest("select id from business_row where group_id = ?", List.of(7L)),
                List.of("id"), Map.of(), "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("secret", tokens)));
    }

    private static ProtectedWriteWork insertWork() {
        return new ProtectedWriteWork(ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into business_row(id, secret) values (?, ?)",
                               List.of(7L, "ciphertext")),
                null,
                List.of("id"),
                Map.of("id", 7L),
                "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(
                        new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[32])),
                        new ProtectedWriteWork.FieldTokens("email", List.of(new byte[32]))));
    }

    private static java.sql.Connection jdbcConnection(Counts counts) {
        return proxy(java.sql.Connection.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "getAutoCommit" -> true;
            case "prepareStatement" -> jdbcStatement((String) arguments[0], counts);
            case "commit" -> { counts.commits++; yield null; }
            case "rollback" -> { counts.rollbacks++; yield null; }
            case "close" -> { counts.closes++; yield null; }
            case "setAutoCommit" -> { counts.begins++; yield null; }
            case "isClosed" -> false;
            case "toString" -> "counting-jdbc-connection";
            default -> defaultValue(method.getReturnType());
        });
    }
    private static PreparedStatement jdbcStatement(String sql, Counts counts) {
        AtomicInteger batches = new AtomicInteger();
        return proxy(PreparedStatement.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "executeQuery" -> { counts.ownerReads++; yield jdbcRows(counts.owners); }
            case "executeLargeUpdate" -> { counts.businessWrites++; yield (long) counts.owners; }
            case "executeUpdate" -> {
                if (!sql.startsWith("delete from token_index")) throw new AssertionError(sql);
                counts.deletes++;
                counts.order.add("D:1");
                yield 1;
            }
            case "addBatch" -> { batches.incrementAndGet(); yield null; }
            case "executeBatch" -> {
                boolean insert = sql.startsWith("insert into token_index");
                if (insert) {
                    counts.inserts++;
                } else {
                    counts.deletes++;
                }
                counts.order.add((insert ? "I:" : "D:") + batches.get());
                yield IntStream.range(0, batches.get())
                        .map(index -> insert && counts.invalidInsert ? 0 : 1).toArray();
            }
            case "isClosed" -> false;
            case "toString" -> sql;
            default -> defaultValue(method.getReturnType());
        });
    }
    private static ResultSet jdbcRows(int ownerCount) {
        AtomicInteger row = new AtomicInteger();
        ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "getColumnCount" -> 1;
            case "getColumnLabel", "getColumnName" -> "id";
            case "getColumnType" -> Types.BIGINT;
            case "getColumnTypeName" -> "BIGINT";
            case "getColumnClassName" -> Long.class.getName();
            default -> defaultValue(method.getReturnType());
        });
        return proxy(ResultSet.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "getMetaData" -> metadata;
            case "next" -> row.incrementAndGet() <= ownerCount;
            case "getObject", "getLong" -> (long) row.get();
            case "wasNull", "isClosed" -> false;
            default -> defaultValue(method.getReturnType());
        });
    }
    private static Connection r2dbcConnection(Counts counts) {
        return proxy(Connection.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "isAutoCommit" -> true;
            case "createStatement" -> r2dbcStatement((String) arguments[0], counts);
            case "beginTransaction" -> { counts.begins++; yield Mono.empty(); }
            case "commitTransaction" -> { counts.commits++; yield Mono.empty(); }
            case "rollbackTransaction" -> { counts.rollbacks++; yield Mono.empty(); }
            case "close" -> { counts.closes++; yield Mono.empty(); }
            case "setAutoCommit" -> Mono.empty();
            case "toString" -> "counting-r2dbc-connection";
            default -> throw new UnsupportedOperationException(method.getName());
        });
    }
    private static Statement r2dbcStatement(String sql, Counts counts) {
        AtomicInteger adds = new AtomicInteger();
        return proxy(Statement.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "bind", "bindNull", "fetchSize" -> proxy;
            case "add" -> { adds.incrementAndGet(); yield proxy; }
            case "execute" -> {
                if (sql.startsWith("select")) {
                    counts.ownerReads++;
                    yield Mono.just(r2dbcRows(counts.owners));
                }
                if (sql.startsWith("update")) {
                    counts.businessWrites++;
                    yield Mono.just(rowsUpdated(counts.owners));
                }
                if (sql.startsWith("delete")) {
                    counts.deletes++;
                    counts.order.add("D:" + (adds.get() + 1));
                    yield Mono.just(rowsUpdated(adds.get() + 1));
                }
                if (sql.startsWith("insert")) {
                    if (sql.startsWith("insert into business_row")) {
                        counts.businessWrites++;
                        yield Mono.just(rowsUpdated(1));
                    }
                    counts.inserts++;
                    counts.order.add("I:" + (adds.get() + 1));
                    yield Mono.just(rowsUpdated(counts.invalidInsert ? 0 : adds.get() + 1));
                }
                throw new AssertionError(sql);
            }
            case "toString" -> sql;
            default -> throw new UnsupportedOperationException(method.getName());
        });
    }
    private static Result rowsUpdated(long count) {
        return proxy(Result.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "getRowsUpdated" -> Mono.just(count);
            case "toString" -> "rows-updated";
            default -> throw new UnsupportedOperationException(method.getName());
        });
    }
    @SuppressWarnings("unchecked")
    private static Result r2dbcRows(int ownerCount) {
        ColumnMetadata column = proxy(ColumnMetadata.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "getName" -> "id";
            case "getType" -> R2dbcType.BIGINT;
            case "getJavaType" -> Long.class;
            case "getNullability" -> Nullability.NON_NULL;
            default -> defaultValue(method.getReturnType());
        });
        RowMetadata metadata = proxy(RowMetadata.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "getColumnMetadatas" -> List.of(column);
            case "getColumnMetadata" -> column;
            case "contains" -> true;
            default -> defaultValue(method.getReturnType());
        });
        return proxy(Result.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "map" -> Flux.range(1, ownerCount).map(id -> {
                Row row = proxy(Row.class, (rowProxy, rowMethod, rowArguments) -> switch (rowMethod.getName()) {
                    case "get" -> (long) id;
                    case "getMetadata" -> metadata;
                    default -> defaultValue(rowMethod.getReturnType());
                });
                return ((BiFunction<Row, RowMetadata, Object>) arguments[0]).apply(row, metadata);
            });
            case "toString" -> "owner-rows";
            default -> throw new UnsupportedOperationException(method.getName());
        });
    }
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) return null;
        if (type == boolean.class) return false;
        if (type == char.class) return '\0';
        if (type == long.class) return 0L;
        if (type == float.class) return 0F;
        if (type == double.class) return 0D;
        return 0;
    }
}
