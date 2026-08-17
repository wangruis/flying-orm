package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.isolation.IsolationContext;
import com.flying.orm.rdb.isolation.IsolationContexts;
import com.flying.orm.rdb.isolation.R2dbcConnectionInvalidator;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionLogObserver;
import com.flying.orm.rdb.observation.SqlExecutionLogOptions;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionResultKind;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionCompletion;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipationException;
import com.flying.orm.rdb.transaction.TransactionOutcome;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 R2DBC 执行器使用真实响应式 Publisher 契约执行 SQL 请求。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
class R2dbcSqlExecutorTest {

    /** 受保护业务写和 CONTAINS 令牌必须在一个冷 R2DBC 事务中顺序提交。 */
    @Test
    void commitsProtectedBusinessWriteAndContainsTokensInOneReactiveTransaction() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        List<SqlExecutionObservation> observations = new ArrayList<>();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory).withObserver(observations::add);
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into protected_customer(id, contact) values (?, ?)",
                               List.of(1L, new byte[]{9, 8, 7})),
                null,
                List.of("id"),
                Map.of("id", 1L),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1, 2, 3}))));

        StepVerifier.create(executor.atomicProtectedWrite(work, SqlExecutionOptions.safeDefaults()))
                    .assertNext(result -> assertEquals(1L, result.affectedRows()))
                    .verifyComplete();

        assertEquals(1, factory.beginCount());
        assertEquals(1, factory.commitCount());
        assertEquals(0, factory.rollbackCount());
        assertEquals(1, factory.closedCount());
        assertEquals(List.of(work.writeRequest().sql(), work.insertSql()), factory.sqlHistory());
        assertEquals(1, observations.size());
        assertEquals(SqlExecutionOperation.UPDATE, observations.getFirst().operation());
        assertEquals(SqlExecutionStatus.SUCCESS, observations.getFirst().status());
        assertEquals(work.writeRequest().sql(), observations.getFirst().sql());
        assertEquals(1L, observations.getFirst().rows());
    }

    /** 受保护写入的资源清理故障必须按 UPDATE 归类，不能污染分片批量写指标。 */
    @Test
    void classifiesProtectedWriteCleanupFailureAsUpdate() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        AtomicInteger closeAttempts = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(cleanupFailingInvalidator(
                        closeAttempts, invalidations,
                        new IllegalStateException("connection close failed"), null))
                .withObserver(cleanupObserver(cleanupObservations));
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into protected_customer(id, contact) values (?, ?)",
                               List.of(1L, new byte[]{9, 8, 7})),
                null, List.of("id"), Map.of("id", 1L),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1, 2, 3}))));

        StepVerifier.create(executor.atomicProtectedWrite(work, SqlExecutionOptions.safeDefaults()))
                    .assertNext(result -> assertEquals(1L, result.affectedRows()))
                    .verifyComplete();

        assertEquals(1, closeAttempts.get());
        assertEquals(1, invalidations.get());
        assertEquals(1, cleanupObservations.size());
        assertEquals(SqlExecutionOperation.UPDATE, cleanupObservations.getFirst().operation());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                     cleanupObservations.getFirst().phase());
        assertTrue(cleanupObservations.getFirst().outcomeConfirmed());
    }

    /** 显式空自增主键仍须使用数据库生成键补齐响应式侧索引 owner。 */
    @Test
    void replacesNullOwnerWithGeneratedKeyForProtectedContainsTokens() {
        Map<String, Object> knownOwner = new LinkedHashMap<>();
        knownOwner.put("id", null);
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(Map.of("id", 42L)), 1L);
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into protected_customer(contact) values (?)", List.of(new byte[]{9})),
                null,
                List.of("id"),
                knownOwner,
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1, 2, 3}))));

        StepVerifier.create(R2dbcSqlExecutor.create(factory)
                                             .atomicProtectedWrite(work, SqlExecutionOptions.safeDefaults()))
                    .assertNext(result -> assertEquals(42L, result.generatedKeys().getFirst().value(0)))
                    .verifyComplete();

        assertTrue(factory.binds().stream()
                          .filter(ValueBind.class::isInstance)
                          .map(ValueBind.class::cast)
                          .anyMatch(bind -> bind.index() == 0 && Long.valueOf(42L).equals(bind.value())));
        assertEquals(List.of("id"), factory.generatedKeyColumns());
    }

    /** R2DBC 侧索引 INSERT 返回零影响行时必须回滚业务写，不能继续提交。 */
    @Test
    void rollsBackProtectedBusinessWriteWhenContainsTokenInsertAffectsNoRow() {
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into protected_customer(id, contact) values (?, ?)",
                               List.of(1L, new byte[]{9, 8, 7})),
                null,
                List.of("id"),
                Map.of("id", 1L),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1, 2, 3}))));
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1)
                .zeroRowsFor(work.insertSql());
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        StepVerifier.create(executor.atomicProtectedWrite(work, SqlExecutionOptions.safeDefaults()))
                    .expectError()
                    .verify();

        assertEquals(1, factory.beginCount());
        assertEquals(0, factory.commitCount());
        assertEquals(1, factory.rollbackCount());
        assertEquals(1, factory.closedCount());
    }

    /** 显式无限清理必须传到保护写回滚，不能被内部固定五秒边界改写为 UNKNOWN。 */
    @Test
    void protectedWriteHonorsUnlimitedCleanupTimeoutDuringRollback() {
        AssertionError operationFailure = new AssertionError("protected write failed");
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 1, "recording", operationFailure).delayRollback(Duration.ofSeconds(6));
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into protected_customer(id, contact) values (?, ?)",
                               List.of(1L, new byte[]{9, 8, 7})),
                null,
                List.of("id"),
                Map.of("id", 1L),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1, 2, 3}))));
        SqlExecutionOptions options = SqlExecutionOptions.unlimited().withCleanupTimeout(Duration.ZERO);

        StepVerifier.withVirtualTime(() -> R2dbcSqlExecutor.create(factory).atomicProtectedWrite(work, options))
                    .thenAwait(Duration.ofSeconds(6))
                    .expectErrorSatisfies(error -> {
                        RdbException translated = assertInstanceOf(RdbException.class, error);
                        assertEquals("database operation failed", translated.getMessage());
                        assertSame(operationFailure, translated.getCause());
                    })
                    .verify(Duration.ofSeconds(1));

        assertEquals(1, factory.rollbackCount());
    }

    /** 受保护写在拿到连接前也只服从连接池等待策略。 */
    @Test
    void delegatesProtectedWriteConnectionWaitingToThePool() {
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into protected_customer(id, contact) values (?, ?)",
                               List.of(1L, new byte[]{9})),
                null, List.of("id"), Map.of("id", 1L),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1}))));
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                                                         .withTimeout(Duration.ofMillis(10));

        StepVerifier.withVirtualTime(() -> R2dbcSqlExecutor.create(neverConnectionFactory())
                                                           .atomicProtectedWrite(work, options))
                    .thenAwait(Duration.ofSeconds(1))
                    .thenCancel()
                    .verify();
    }

    /** 回滚 VME 即使已经引用业务错误，也必须在资源清理后原样出站且不能形成反向异常环。 */
    @Test
    void promotesProtectedWriteRollbackFatalThatAlreadyReferencesTheOperationFailure() {
        AssertionError operationFailure = new AssertionError("protected write failed");
        OutOfMemoryError rollbackFatal = new OutOfMemoryError("protected rollback failed");
        rollbackFatal.initCause(operationFailure);
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 1, "recording", operationFailure).failRollback(rollbackFatal);
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into protected_customer(id, contact) values (?, ?)",
                               List.of(1L, new byte[]{9, 8, 7})),
                null,
                List.of("id"),
                Map.of("id", 1L),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1, 2, 3}))));

        StepVerifier.create(R2dbcSqlExecutor.create(factory)
                                             .atomicProtectedWrite(work, SqlExecutionOptions.safeDefaults()))
                    .expectErrorMatches(error -> error == rollbackFatal)
                    .verify();

        assertEquals(1, factory.rollbackCount());
        assertSame(operationFailure, rollbackFatal.getCause());
        assertFalse(reaches(operationFailure, rollbackFatal));
    }

    /** R2DBC UPSERT 必须在写入新 CONTAINS 令牌前删除同一 owner 的旧令牌。 */
    @Test
    void replacesContainsTokensWhenProtectedUpsertUpdatesAnExistingOwner() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory);
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPSERT,
                new SqlRequest("merge into protected_customer key(id) values (?, ?)",
                               List.of(1L, new byte[]{9, 8, 7})),
                null,
                List.of("id"),
                Map.of("id", 1L),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1, 2, 3}))));

        StepVerifier.create(executor.atomicProtectedWrite(work, SqlExecutionOptions.safeDefaults()))
                    .assertNext(result -> assertEquals(1L, result.affectedRows()))
                    .verifyComplete();

        assertEquals(List.of(work.writeRequest().sql(), work.deleteSql(), work.insertSql()),
                     factory.sqlHistory());
    }

    /** 外部 R2DBC 事务只借用连接，保护写不能再次 begin、commit 或 close。 */
    @Test
    void enlistsProtectedWriteWithoutControllingTheExternalReactiveTransaction() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        Connection external = factory.connection();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withTransactionParticipant(() -> Mono.just(
                        R2dbcTransactionContext.external(external, "primary")));
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into protected_customer(id, contact) values (?, ?)",
                               List.of(1L, new byte[]{9})),
                null, List.of("id"), Map.of("id", 1L),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1}))));

        StepVerifier.create(executor.atomicProtectedWrite(work, SqlExecutionOptions.safeDefaults()))
                    .assertNext(result -> assertEquals(1L, result.affectedRows()))
                    .verifyComplete();

        assertEquals(0, factory.createdCount());
        assertEquals(0, factory.beginCount());
        assertEquals(0, factory.commitCount());
        assertEquals(0, factory.rollbackCount());
        assertEquals(0, factory.closedCount());
    }

    /** 保护写在 COMMIT 回执超时后必须报告 UNKNOWN，并淘汰结果不确定的自有连接。 */
    @Test
    void reportsUnknownWhenProtectedWriteCommitTimesOut() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1).hangCommit();
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(recordingInvalidator(reusableCloses, invalidations));
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into protected_customer(id, contact) values (?, ?)",
                               List.of(1L, new byte[]{9})),
                null, List.of("id"), Map.of("id", 1L),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1}))));
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults().withTimeout(Duration.ofMillis(25));

        StepVerifier.create(executor.atomicProtectedWrite(work, options))
                    .expectErrorSatisfies(error -> {
                        RdbException failure = assertInstanceOf(RdbException.class, error);
                        assertEquals(RdbErrorKind.UNKNOWN, failure.kind());
                    })
                    .verify(Duration.ofSeconds(2));

        assertEquals(1, factory.beginCount());
        assertEquals(1, factory.commitCount());
        assertEquals(0, factory.rollbackCount());
        assertEquals(0, reusableCloses.get());
        assertEquals(1, invalidations.get());
    }

    /** 连接已取得但 BEGIN 回执超时同样不能伪装成普通执行超时。 */
    @Test
    void reportsUnknownWhenProtectedWriteBeginTimesOut() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1).hangBegin();
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(recordingInvalidator(reusableCloses, invalidations));
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into protected_customer(id, contact) values (?, ?)",
                               List.of(1L, new byte[]{9})),
                null, List.of("id"), Map.of("id", 1L),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1}))));
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults().withTimeout(Duration.ofMillis(25));

        StepVerifier.create(executor.atomicProtectedWrite(work, options))
                    .expectErrorSatisfies(error -> {
                        RdbException failure = assertInstanceOf(RdbException.class, error);
                        assertEquals(RdbErrorKind.UNKNOWN, failure.kind());
                    })
                    .verify(Duration.ofSeconds(2));

        assertEquals(1, factory.beginCount());
        assertEquals(0, factory.commitCount());
        assertEquals(0, factory.rollbackCount());
        assertEquals(0, reusableCloses.get());
        assertEquals(1, invalidations.get());
    }

    /** 事务已 ACTIVE 时保护写超时必须先确认回滚，再保留普通超时语义并归还可复用连接。 */
    @Test
    void rollsBackActiveProtectedWriteWhenExecutionTimesOut() {
        String writeSql = "insert into protected_customer(id, contact) values (?, ?)";
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 1, "recording", null, Duration.ZERO, writeSql);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory);
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest(writeSql, List.of(1L, new byte[]{9})),
                null, List.of("id"), Map.of("id", 1L),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1}))));
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults().withTimeout(Duration.ofMillis(25));

        StepVerifier.create(executor.atomicProtectedWrite(work, options))
                    .expectError(SqlExecutionTimeoutException.class)
                    .verify(Duration.ofSeconds(2));

        assertEquals(1, factory.beginCount());
        assertEquals(0, factory.commitCount());
        assertEquals(1, factory.rollbackCount());
        assertEquals(1, factory.closedCount());
    }

    /**
     * 验证查询请求会创建 R2DBC Statement、绑定参数、映射行数据并关闭连接。
     */
    @Test
    void queriesRowsReactivelyAndClosesConnection() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(row("id", "u1", "name", "王")), 0);
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        StepVerifier.create(executor.query(new SqlRequest("select id, name from Users where status = ?",
                                                          List.of("enabled"))))
                    .assertNext(actual -> {
                        DynamicRow result = assertInstanceOf(DynamicRow.class, actual);
                        assertEquals("u1", result.value(0));
                        assertEquals("王", result.get("name"));
                    })
                    .verifyComplete();

        assertEquals("select id, name from Users where status = ?", factory.sql());
        assertEquals(List.of(new ValueBind(0, "enabled")), factory.binds());
        assertEquals(1, factory.closedCount());
    }

    /**
     * 验证 null 参数不会被丢弃，而是通过 R2DBC bindNull 按索引绑定。
     */
    @Test
    void bindsNullParameterByIndex() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        StepVerifier.create(executor.rowsUpdated(new SqlRequest("update Users set deleted_at = ? where id = ?",
                                                                Arrays.asList(null, "u1"))))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals(List.of(new NullBind(0, Object.class), new ValueBind(1, "u1")), factory.binds());
        assertEquals(1, factory.closedCount());
    }

    /** R2DBC LOB 必须在自有连接关闭前完成物化，原始查询也不能把连接绑定的句柄带出资源域。 */
    @Test
    void materializesLargeObjectsBeforeClosingOwnedConnection() {
        AtomicReference<RecordingConnectionFactory> factoryRef = new AtomicReference<>();
        Blob blob = Blob.from(Flux.defer(() -> {
            if (factoryRef.get().closedCount() != 0) {
                return Flux.error(new IllegalStateException("connection closed before Blob consumption"));
            }
            return Flux.just(ByteBuffer.wrap(new byte[]{1, 2, 3}));
        }));
        Clob clob = Clob.from(Flux.defer(() -> {
            if (factoryRef.get().closedCount() != 0) {
                return Flux.error(new IllegalStateException("connection closed before Clob consumption"));
            }
            return Flux.just("large text");
        }));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("payload", blob);
        values.put("content", clob);
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(values), 0);
        factoryRef.set(factory);

        StepVerifier.create(R2dbcSqlExecutor.create(factory).query(
                            new SqlRequest("select payload, content from documents", List.of())))
                    .assertNext(row -> {
                        assertTrue(Arrays.equals(new byte[]{1, 2, 3}, (byte[]) row.get("payload")));
                        assertEquals("large text", row.get("content"));
                        assertEquals(0, factory.closedCount());
                    })
                    .verifyComplete();

        assertEquals(1, factory.closedCount());
    }

    /** 取消活动 LOB 时，连接必须等待后续未订阅 locator 的异步 discard 完成后才能归还。 */
    @Test
    void waitsForPendingLargeObjectDiscardBeforeClosingOwnedConnectionOnCancel() {
        AtomicInteger discards = new AtomicInteger();
        Sinks.Empty<Void> discardGate = Sinks.empty();
        Blob blob = Blob.from(Flux.never());
        Clob clob = new Clob() {
            @Override
            public Publisher<CharSequence> stream() {
                return Flux.just("unused");
            }

            @Override
            public Publisher<Void> discard() {
                discards.incrementAndGet();
                return discardGate.asMono();
            }
        };
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("payload", blob);
        values.put("content", clob);
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(values), 0);

        var executor = R2dbcSqlExecutor.create(factory).withConnectionInvalidator(
                R2dbcConnectionInvalidator.of(Connection::close, Connection::close));
        var subscription = executor.query(
                new SqlRequest("select payload, content from documents", List.of())).subscribe();
        subscription.dispose();

        assertEquals(1, discards.get());
        assertEquals(0, factory.closedCount());
        discardGate.tryEmitEmpty();
        assertEquals(1, factory.closedCount());
    }

    /** 驱动分段回报的影响行数超出 long 时必须失败，不能回绕为负数或饱和伪报。 */
    @Test
    void rejectsOverflowingRowsUpdatedAcrossDriverResults() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), List.of(Long.MAX_VALUE, 1L));

        StepVerifier.create(R2dbcSqlExecutor.create(factory).rowsUpdated(
                            new SqlRequest("update Users set enabled = ?", List.of(true))))
                    .expectErrorMatches(error -> error instanceof RdbException rdbError
                            && rdbError.kind() == RdbErrorKind.UNKNOWN
                            && "database execution count exceeds supported range".equals(rdbError.getMessage())
                            && rdbError.getCause() instanceof ArithmeticException)
                    .verify();
    }

    /**
     * 数据库已经报告写入成功后，连接关闭故障只能作为资源清理故障观测，不能覆盖成功结果。
     * 否则业务层会把已经生效的写入当成失败重试，造成重复数据。
     */
    @Test
    void keepsSuccessfulUpdateResultWhenConnectionCloseFails() {
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(),
                1,
                "recording",
                null,
                Duration.ZERO,
                null,
                new IllegalStateException("connection close failed"));
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(recordingInvalidator(reusableCloses, invalidations))
                .withObserver(new SqlExecutionObserver() {
            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // 本测试只关心资源清理故障。
            }

            @Override
            public void onResourceCleanup(ResourceCleanupObservation observation) {
                cleanupObservations.add(observation);
            }
        });

        StepVerifier.create(executor.rowsUpdated(new SqlRequest(
                            "update Users set name = ? where id = ?",
                            List.of("王", "u1"))))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals(1, factory.closedCount());
        assertEquals(1, reusableCloses.get());
        assertEquals(1, invalidations.get());
        assertEquals(1, cleanupObservations.size());
        assertEquals(SqlExecutionOperation.UPDATE, cleanupObservations.getFirst().operation());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                     cleanupObservations.getFirst().phase());
        assertTrue(cleanupObservations.getFirst().outcomeConfirmed());
    }

    /** 关闭 Publisher 永不结束时，独立清理上限必须释放成功写入结果并发布一次超时观测。 */
    @Test
    void completesSuccessfulUpdateWhenConnectionCloseNeverCompletes() {
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 1, "recording", null, Duration.ZERO, null, null, Duration.ZERO, true);
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(recordingInvalidator(reusableCloses, invalidations))
                .withObserver(cleanupObserver(cleanupObservations));
        SqlExecutionOptions options = SqlExecutionOptions.unlimited()
                                                         .withCleanupTimeout(Duration.ofMillis(50));

        StepVerifier.withVirtualTime(() -> executor.rowsUpdated(
                            new SqlRequest("update Users set enabled = true", List.of()), options))
                    .thenAwait(Duration.ofMillis(51))
                    .expectNext(1L)
                    .expectComplete()
                    .verify(Duration.ofSeconds(1));

        assertEquals(1, cleanupObservations.size());
        ResourceCleanupObservation observation = cleanupObservations.getFirst();
        assertEquals(SqlExecutionOperation.UPDATE, observation.operation());
        assertTrue(observation.outcomeConfirmed());
        assertEquals(ResourceCleanupObservation.FailureKind.TIMEOUT, observation.failureKind());
        assertEquals(1, reusableCloses.get());
        assertEquals(1, invalidations.get());
    }

    /** 成功查询的行已经成为数据库事实，后续关闭失败只能进入资源观测。 */
    @Test
    void keepsSuccessfulQueryResultWhenConnectionCloseFails() {
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(row("id", "u1")),
                0,
                "recording",
                null,
                Duration.ZERO,
                null,
                new IllegalStateException("connection close failed"));
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withObserver(cleanupObserver(cleanupObservations));

        StepVerifier.create(executor.query(new SqlRequest("select id from Users", List.of())))
                    .assertNext(row -> assertEquals("u1", row.get("id")))
                    .verifyComplete();

        assertEquals(1, cleanupObservations.size());
        assertEquals(SqlExecutionOperation.QUERY, cleanupObservations.getFirst().operation());
        assertTrue(cleanupObservations.getFirst().outcomeConfirmed());
    }

    /** 清理观测只能说明资源阶段失败，不得把驱动错误中的参数、租户或连接凭据继续向外传播。 */
    @Test
    void cleanupObservationSanitizesSensitiveDriverFailure() {
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        IllegalStateException closeFailure = new IllegalStateException(
                "close failed: password=db-secret tenant=tenant-7 parameter=private-value");
        closeFailure.addSuppressed(new IllegalStateException("invalidate failed for tenant=tenant-7"));
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(),
                1,
                "recording",
                null,
                Duration.ZERO,
                null,
                closeFailure);
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(R2dbcConnectionInvalidator.of(
                        Connection::close, connection -> Mono.empty()))
                .withObserver(cleanupObserver(cleanupObservations));

        StepVerifier.create(executor.rowsUpdated(new SqlRequest(
                            "update Users set name = ?", List.of("private-value"))))
                    .expectNext(1L)
                    .verifyComplete();

        String message = cleanupObservations.getFirst().error().getMessage();
        assertEquals(ResourceCleanupObservation.FailureKind.FAILURE,
                     cleanupObservations.getFirst().failureKind());
        assertFalse(message.contains("db-secret"));
        assertFalse(message.contains("tenant-7"));
        assertFalse(message.contains("private-value"));
        assertEquals(1, cleanupObservations.getFirst().error().getSuppressed().length);
        assertFalse(cleanupObservations.getFirst().error().getSuppressed()[0].getMessage().contains("tenant-7"));
    }

    /** SQL 操作截止时间在数据库结果产生时停止计时，不能在后续连接关闭阶段重新生效。 */
    @Test
    void operationTimeoutDoesNotIncludeResourceCleanupTime() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 1, "recording", null, Duration.ZERO, null, null, Duration.ofMillis(150), false);
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);
        SqlExecutionOptions options = SqlExecutionOptions.timeout(Duration.ofMillis(100))
                                                         .withCleanupTimeout(Duration.ofMillis(200));

        StepVerifier.withVirtualTime(() -> executor.rowsUpdated(
                            new SqlRequest("update Users set enabled = true", List.of()), options))
                    .thenAwait(Duration.ofMillis(150))
                    .expectNext(1L)
                    .verifyComplete();
    }

    @Test
    void executesSetupWorkAndCleanupOnOneConnection() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        ConnectionScopedReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);
        SqlExecutionSequence sequence = new SqlExecutionSequence(
                List.of(new SqlRequest("set lock_timeout = '1500ms'", List.of())),
                List.of(new SqlRequest("alter table users add column email varchar(255)", List.of()),
                        new SqlRequest("create index idx_users_email on users (email)", List.of())),
                List.of(new SqlRequest("reset lock_timeout", List.of())));

        StepVerifier.create(executor.executeInConnection(sequence, SqlExecutionOptions.unlimited()))
                    .assertNext(result -> {
                        assertEquals(2, result.workSteps().size());
                        assertEquals(2L, result.rowsUpdated());
                    })
                    .verifyComplete();

        assertEquals(List.of("set lock_timeout = '1500ms'",
                             "alter table users add column email varchar(255)",
                             "create index idx_users_email on users (email)",
                             "reset lock_timeout"),
                     factory.sqlHistory());
        assertEquals(1, factory.createdCount());
        assertEquals(1, factory.closedCount());
    }

    /** DDL 业务步骤已完成后，最终连接关闭失败不能覆盖已完成步骤与影响行数。 */
    @Test
    void keepsCompletedDdlResultWhenFinalConnectionCloseFails() {
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(),
                1,
                "recording",
                null,
                Duration.ZERO,
                null,
                new IllegalStateException("connection close failed"));
        ConnectionScopedReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withObserver(cleanupObserver(cleanupObservations));
        SqlExecutionSequence sequence = new SqlExecutionSequence(
                List.of(),
                List.of(new SqlRequest("alter table users add active boolean", List.of())),
                List.of());

        StepVerifier.create(executor.executeInConnection(
                            sequence, SqlExecutionOptions.safeDefaults()))
                    .assertNext(result -> assertEquals(1L, result.rowsUpdated()))
                    .verifyComplete();

        assertEquals(1, cleanupObservations.size());
        assertTrue(cleanupObservations.getFirst().outcomeConfirmed());
    }

    /** DDL 完成后的最终关闭即使永久挂起，也必须在独立清理上限内返回已完成结果。 */
    @Test
    void keepsCompletedDdlResultWhenFinalConnectionCloseNeverCompletes() {
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 1, "recording", null, Duration.ZERO, null, null, Duration.ZERO, true);
        ConnectionScopedReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withObserver(cleanupObserver(cleanupObservations));
        SqlExecutionSequence sequence = new SqlExecutionSequence(
                List.of(),
                List.of(new SqlRequest("alter table users add active boolean", List.of())),
                List.of());
        SqlExecutionOptions options = SqlExecutionOptions.unlimited()
                                                         .withCleanupTimeout(Duration.ofMillis(50));

        StepVerifier.withVirtualTime(() -> executor.executeInConnection(sequence, options))
                    .thenAwait(Duration.ofMillis(50))
                    .assertNext(result -> assertEquals(1L, result.rowsUpdated()))
                    .expectComplete()
                    .verify(Duration.ofSeconds(1));

        assertEquals(1, cleanupObservations.size());
        assertTrue(cleanupObservations.getFirst().outcomeConfirmed());
    }

    @Test
    void reportsSetupFailureAndStillAttemptsCleanup() {
        IllegalStateException setupFailure = new IllegalStateException("setup failed");
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 0, "PostgreSQL", setupFailure);
        ConnectionScopedReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(recordingInvalidator(reusableCloses, invalidations))
                .withObserver(cleanupObserver(cleanupObservations));
        SqlExecutionSequence sequence = new SqlExecutionSequence(
                List.of(new SqlRequest("set lock_timeout = '1500ms'", List.of())),
                List.of(new SqlRequest("alter table users add column email varchar(255)", List.of())),
                List.of(new SqlRequest("reset lock_timeout", List.of())));

        StepVerifier.create(executor.executeInConnection(sequence, SqlExecutionOptions.unlimited()))
                    .expectErrorSatisfies(error -> {
                        SqlExecutionSequenceException failure = (SqlExecutionSequenceException) error;
                        assertEquals(SqlExecutionPhase.SETUP, failure.phase());
                        assertEquals(0, failure.stepIndex());
                        assertEquals(List.of(), failure.completedWorkSteps());
                        assertSame(setupFailure, failure.getCause());
                        assertEquals(1, failure.getSuppressed().length);
                        SqlExecutionSequenceException cleanupFailure = assertInstanceOf(
                                SqlExecutionSequenceException.class, failure.getSuppressed()[0]);
                        assertEquals(SqlExecutionPhase.CLEANUP, cleanupFailure.phase());
                    })
                    .verify();

        assertEquals(List.of("set lock_timeout = '1500ms'", "reset lock_timeout"), factory.sqlHistory());
        assertEquals(0, reusableCloses.get());
        assertEquals(0, factory.closedCount());
        assertEquals(1, invalidations.get());
        assertEquals(1, cleanupObservations.size());
        assertEquals(ResourceCleanupObservation.Phase.SESSION_CLEANUP,
                     cleanupObservations.getFirst().phase());
        assertFalse(cleanupObservations.getFirst().outcomeConfirmed());
    }

    @Test
    void cancellationRunsSessionCleanupBeforeClosingConnection() {
        String workSql = "alter table users add column email varchar(255)";
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 0, "PostgreSQL", null, Duration.ZERO, workSql);
        ConnectionScopedReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(recordingInvalidator(reusableCloses, invalidations));
        SqlExecutionSequence sequence = new SqlExecutionSequence(
                List.of(),
                List.of(new SqlRequest(workSql, List.of())),
                List.of(new SqlRequest("reset lock_timeout", List.of())));

        StepVerifier.create(executor.executeInConnection(sequence, SqlExecutionOptions.unlimited()))
                    .thenAwait(Duration.ofMillis(10))
                    .thenCancel()
                    .verify();

        assertEquals(List.of(workSql, "reset lock_timeout"), factory.sqlHistory());
        assertEquals(1, factory.createdCount());
        assertEquals(0, factory.closedCount());
        assertEquals(0, reusableCloses.get());
        assertEquals(1, invalidations.get());
    }

    /** DDL 取消后的会话 cleanup 失败表示连接状态未知，必须观测失败并失效，禁止普通归池。 */
    @Test
    void cancellationCleanupFailureIsObservedAndInvalidatedInsteadOfClosed() {
        String workSql = "alter table users add column email varchar(255)";
        IllegalStateException cleanupFailure = new IllegalStateException(
                "reset failed: tenant=private-tenant password=private-password");
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 0, "PostgreSQL", cleanupFailure, Duration.ZERO, workSql);
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        ConnectionScopedReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(recordingInvalidator(reusableCloses, invalidations))
                .withObserver(cleanupObserver(cleanupObservations));
        SqlExecutionSequence sequence = new SqlExecutionSequence(
                List.of(),
                List.of(new SqlRequest(workSql, List.of())),
                List.of(new SqlRequest("reset lock_timeout", List.of())));

        StepVerifier.create(executor.executeInConnection(sequence, SqlExecutionOptions.safeDefaults()))
                    .thenAwait(Duration.ofMillis(10))
                    .thenCancel()
                    .verify();

        assertEquals(List.of(workSql, "reset lock_timeout"), factory.sqlHistory());
        assertEquals(0, reusableCloses.get());
        assertEquals(1, invalidations.get());
        assertEquals(1, cleanupObservations.size());
        ResourceCleanupObservation observation = cleanupObservations.getFirst();
        assertEquals(ResourceCleanupObservation.Phase.SESSION_CLEANUP, observation.phase());
        assertEquals(ResourceCleanupObservation.FailureKind.FAILURE, observation.failureKind());
        assertFalse(observation.outcomeConfirmed());
        assertFalse(observation.error().getMessage().contains("private-tenant"));
    }

    @Test
    void defaultAndObservationWrappersKeepConnectionScopedCapability() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        ReactiveSqlExecutor wrapped = R2dbcSqlExecutor.create(factory)
                .withDefaultExecutionOptions(SqlExecutionOptions.timeout(Duration.ofSeconds(2)))
                .withObserver(ignored -> {
                });

        ConnectionScopedReactiveSqlExecutor scoped = (ConnectionScopedReactiveSqlExecutor) wrapped;
        StepVerifier.create(scoped.executeInConnection(
                            new SqlExecutionSequence(List.of(),
                                                     List.of(new SqlRequest("alter table users add active boolean", List.of())),
                                                     List.of()),
                            SqlExecutionOptions.unlimited()))
                    .assertNext(result -> assertEquals(1L, result.rowsUpdated()))
                    .verifyComplete();

        assertEquals(1, factory.createdCount());
        assertEquals(1, factory.closedCount());
    }

    /**
     * 验证批量写入复用一个 Statement，并通过 add 提交多组绑定参数。
     */
    @Test
    void executesBatchWithOneStatementAndMultipleBindings() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 2);
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id, name) values (?, ?)",
                2,
                List.of(String.class, String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{"u1", null}, new Object[]{"u2", "李"}),
                BatchWriteOptions.atomic(2));

        StepVerifier.create(executor.writeBatch(request))
                    .assertNext(result -> assertEquals(2L, result.affectedRows()))
                    .verifyComplete();

        assertEquals("insert into Users (id, name) values (?, ?)", factory.sql());
        assertEquals(List.of(new ValueBind(0, "u1"), new NullBind(1, String.class),
                             new ValueBind(0, "u2"), new ValueBind(1, "李")), factory.binds());
        assertEquals(1, factory.addCount());
        assertEquals(1, factory.closedCount());
    }

    /** 显式开启参数日志后，执行层把本次请求参数交给安全日志 observer，但日志里仍不能出现参数原文。 */
    @Test
    void passesParametersToEnabledSafeSqlLogging() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        List<String> messages = new ArrayList<>();
        SqlExecutionLogObserver logger = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults().withParameters(true), messages::add);
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory).withObserver(logger);

        StepVerifier.create(executor.rowsUpdated(new SqlRequest(
                            "update Users set name = ? where id = ?", List.of("secret-name", "user-100"))))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals(1, messages.size());
        assertTrue(messages.getFirst().contains("parameters=[\"s*********e\", \"u******0\"]"));
        assertFalse(messages.getFirst().contains("secret-name"));
        assertFalse(messages.getFirst().contains("user-100"));
    }

    /**
     * 外部事务已经给出连接后，普通 SQL 只能借用这条连接。备用连接池不能被提前触发，借来的连接也不能被关闭。
     */
    @Test
    void usesExternalTransactionConnectionWithoutCreatingOrClosingAnotherConnection() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        Connection externalConnection = factory.connection();
        AtomicInteger transactionLookups = new AtomicInteger();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory).withTransactionParticipant(() -> {
            transactionLookups.incrementAndGet();
            return Mono.just(R2dbcTransactionContext.external(externalConnection, "primary"));
        });

        Mono<Long> update = IsolationContexts.with(
                executor.rowsUpdated(new SqlRequest(
                        "update Users set name = ? where id = ?", List.of("Wang", "u1"))),
                IsolationContext.database("tenant-7", "primary"));

        assertEquals(0, transactionLookups.get(), "事务上下文必须到订阅时才读取");
        assertEquals(0, factory.createdCount(), "组装响应式链时不能提前申请备用连接");
        StepVerifier.create(update).expectNext(1L).verifyComplete();

        assertEquals(1, transactionLookups.get());
        assertEquals(0, factory.createdCount());
        assertEquals(0, factory.closedCount());
    }

    /** 事务开始后切换数据库必须在创建 Statement 前失败，不能静默把 SQL 发到旧事务连接。 */
    @Test
    void rejectsDatabaseRouteChangeBeforeExecutingSqlInsideExternalTransaction() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        Connection externalConnection = factory.connection();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory).withTransactionParticipant(
                () -> Mono.just(R2dbcTransactionContext.external(externalConnection, "tenant-db-a")));

        Mono<Long> update = IsolationContexts.with(
                executor.rowsUpdated(new SqlRequest("update Users set name = ? where id = ?", List.of("Wang", "u1"))),
                IsolationContext.database("tenant-7", "tenant-db-b"));

        StepVerifier.create(update)
                    .expectErrorSatisfies(error -> {
                        R2dbcTransactionParticipationException rejected = assertInstanceOf(
                                R2dbcTransactionParticipationException.class, error);
                        assertEquals(R2dbcTransactionParticipationException.Reason.ROUTING_IDENTITY_CHANGED,
                                     rejected.reason());
                    })
                    .verify();
        assertEquals(0, factory.createdCount());
        assertTrue(factory.sqlHistory().isEmpty());
        assertEquals(0, factory.closedCount());
    }

    /**
     * ATOMIC 加入外部事务后只负责执行 SQL，不能再次 begin/commit/rollback。外层尚未提交时结果只能是 ENLISTED。
     */
    @Test
    void enlistsAtomicBatchWithoutControllingExternalTransaction() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 2);
        Connection externalConnection = factory.connection();
        AtomicReference<R2dbcTransactionCompletion.Listener> completion = new AtomicReference<>();
        List<BatchExecutionObservation> observations = new ArrayList<>();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withBatchObserver(observations::add)
                .withTransactionParticipant(() -> Mono.just(R2dbcTransactionContext.external(
                        externalConnection, "primary", listener -> completion.compareAndSet(null, listener))));
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{"u1"}, new Object[]{"u2"}),
                BatchWriteOptions.atomic(2));

        StepVerifier.create(executor.writeBatch(request))
                    .assertNext(result -> {
                        assertEquals(BatchWriteResult.Status.ENLISTED, result.status());
                        assertEquals(0L, result.affectedRows());
                        assertEquals(1, result.chunks().size());
                        assertEquals(2, result.inputCount());
                        assertTrue(result.chunks().stream()
                                         .allMatch(chunk -> chunk.status() == BatchChunkResult.Status.ENLISTED));
                    })
                    .verifyComplete();

        StepVerifier.create(Mono.from(completion.get().afterCompletion(
                            TransactionOutcome.COMMITTED)))
                    .verifyComplete();
        assertTrue(observations.stream().anyMatch(observation ->
                observation.summaryStatus() == BatchWriteResult.Status.COMMITTED
                        && observation.affectedRows() == 2L));

        assertEquals(0, factory.createdCount());
        assertEquals(0, factory.beginCount());
        assertEquals(0, factory.commitCount());
        assertEquals(0, factory.rollbackCount());
        assertEquals(0, factory.closedCount());
    }

    /**
     * 外部事务里的 ATOMIC 一旦执行失败必须把错误抛给事务管理器，由外层决定回滚；ORM 不能抢先回滚或关闭连接。
     */
    @Test
    void propagatesAtomicFailureWithoutRollingBackExternalTransaction() {
        RuntimeException driverFailure = new RuntimeException("write failed");
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 0, "recording", driverFailure);
        Connection externalConnection = factory.connection();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory).withTransactionParticipant(
                () -> Mono.just(R2dbcTransactionContext.external(externalConnection, "primary")));
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{"u1"}),
                BatchWriteOptions.atomic(1));

        StepVerifier.create(executor.writeBatch(request))
                    .expectErrorSatisfies(error -> {
                        BatchWriteException failure = assertInstanceOf(BatchWriteException.class, error);
                        assertSame(driverFailure, failure.getCause());
                        assertEquals(BatchWriteResult.Status.UNKNOWN, failure.result().status());
                    })
                    .verify();

        assertEquals(0, factory.createdCount());
        assertEquals(0, factory.beginCount());
        assertEquals(0, factory.commitCount());
        assertEquals(0, factory.rollbackCount());
        assertEquals(0, factory.closedCount());
    }

    /**
     * INDEPENDENT 必须在获取连接、订阅批量输入和执行 SQL 前拒绝，否则分片提交会破坏外部事务的原子边界。
     */
    @Test
    void rejectsIndependentBatchBeforeConnectionInputAndSqlInsideExternalTransaction() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        Connection externalConnection = factory.connection();
        AtomicInteger inputSubscriptions = new AtomicInteger();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory).withTransactionParticipant(
                () -> Mono.just(R2dbcTransactionContext.external(externalConnection, "primary")));
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.defer(() -> {
                    inputSubscriptions.incrementAndGet();
                    return Flux.<Object[]>just(new Object[]{"u1"});
                }),
                BatchWriteOptions.independent(1));

        StepVerifier.create(executor.writeBatch(request))
                    .expectErrorSatisfies(error -> {
                        R2dbcTransactionParticipationException rejected = assertInstanceOf(
                                R2dbcTransactionParticipationException.class, error);
                        assertEquals(R2dbcTransactionParticipationException.Reason.INDEPENDENT_BATCH_NOT_ALLOWED,
                                     rejected.reason());
                    })
                    .verify();

        assertEquals(0, factory.createdCount());
        assertEquals(0, inputSubscriptions.get());
        assertTrue(factory.sqlHistory().isEmpty());
        assertEquals(0, factory.beginCount());
        assertEquals(0, factory.commitCount());
        assertEquals(0, factory.rollbackCount());
        assertEquals(0, factory.closedCount());
    }

    /** ATOMIC 批量也必须在订阅输入流之前检查事务路由，避免先消耗上游数据再发现切库。 */
    @Test
    void rejectsAtomicBatchRouteChangeBeforeInputAndSqlInsideExternalTransaction() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        Connection externalConnection = factory.connection();
        AtomicInteger inputSubscriptions = new AtomicInteger();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory).withTransactionParticipant(
                () -> Mono.just(R2dbcTransactionContext.external(externalConnection, "tenant-db-a")));
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.defer(() -> {
                    inputSubscriptions.incrementAndGet();
                    return Flux.<Object[]>just(new Object[]{"u1"});
                }),
                BatchWriteOptions.atomic(1));

        Mono<BatchWriteResult> write = IsolationContexts.with(
                executor.writeBatch(request), IsolationContext.database("tenant-7", "tenant-db-b"));

        StepVerifier.create(write)
                    .expectErrorSatisfies(error -> {
                        R2dbcTransactionParticipationException rejected = assertInstanceOf(
                                R2dbcTransactionParticipationException.class, error);
                        assertEquals(R2dbcTransactionParticipationException.Reason.ROUTING_IDENTITY_CHANGED,
                                     rejected.reason());
                    })
                    .verify();
        assertEquals(0, factory.createdCount());
        assertEquals(0, inputSubscriptions.get());
        assertTrue(factory.sqlHistory().isEmpty());
    }

    /**
     * 验证 PostgreSQL 驱动拿到的是自己认识的编号参数标记。
     */
    @Test
    void adaptsCanonicalParameterMarkersForPostgresql() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 2, "PostgreSQL");
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id, name) values (?, ?)",
                2,
                List.of(String.class, String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{"u1", "王"}, new Object[]{"u2", "李"}),
                BatchWriteOptions.atomic(2));

        StepVerifier.create(executor.writeBatch(request))
                    .assertNext(result -> assertEquals(2L, result.affectedRows()))
                    .verifyComplete();

        assertEquals("insert into Users (id, name) values ($1, $2)", factory.sql());
        assertEquals(1, factory.addCount());
        assertEquals(1, factory.closedCount());
    }

    /**
     * SQL Server 的 R2DBC 驱动不识别通用问号参数，这里保证执行前会换成驱动认识的 @P 编号。
     */
    @Test
    void adaptsCanonicalParameterMarkersForSqlServer() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1, "Microsoft SQL Server");
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        StepVerifier.create(executor.rowsUpdated(new SqlRequest(
                            "update Users set name = ? where id = ?",
                            List.of("Wang", "u1"))))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("update Users set name = @P0 where id = @P1", factory.sql());
        assertEquals(1, factory.closedCount());
    }

    /**
     * 验证数据库原生 SQL 会完整透传，PostgreSQL 的问号运算符不能被当成参数标记。
     */
    @Test
    void keepsNativePostgresqlQuestionMarkOperator() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 0, "PostgreSQL");
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        StepVerifier.create(executor.query(SqlRequest.nativeSql(
                            "select payload from Events where payload ? 'enabled' and id = $1",
                            List.of("e1"))))
                    .verifyComplete();

        assertEquals("select payload from Events where payload ? 'enabled' and id = $1", factory.sql());
        assertEquals(List.of(new ValueBind(0, "e1")), factory.binds());
        assertEquals(1, factory.closedCount());
    }

    /**
     * 成功执行时会发出轻量观测结果，不包含参数值，只记录类型、耗时和行数。
     */
    @Test
    void observesSuccessfulQueryExecution() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(row("id", "u1")), 0);
        List<SqlExecutionObservation> observations = new ArrayList<>();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory).withObserver(observations::add);

        StepVerifier.create(executor.query(new SqlRequest("select id from Users where status = ?",
                                                          List.of("enabled"))))
                    .expectNext(row("id", "u1"))
                    .verifyComplete();

        assertEquals(1, observations.size());
        SqlExecutionObservation observation = observations.getFirst();
        assertEquals(SqlExecutionOperation.QUERY, observation.operation());
        assertEquals(SqlStatementType.SELECT, observation.statementType());
        assertEquals(SqlExecutionStatus.SUCCESS, observation.status());
        assertEquals(SqlFailureCategory.NONE, observation.failureCategory());
        assertEquals(SqlExecutionResultKind.SUCCESS, observation.resultKind());
        assertEquals(1, observation.parameterCount());
        assertEquals(1, observation.rows());
    }

    /**
     * 失败执行时会记录异常分类，上层不用到处解析驱动异常。
     */
    @Test
    void observesFailedExecutionWithTranslatedFailureCategory() {
        RdbException error = new RdbException(RdbErrorKind.BAD_SQL,
                                             "bad sql",
                                             "42000",
                                             0,
                                             new RuntimeException("driver failed"));
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 0, "recording", error);
        List<SqlExecutionObservation> observations = new ArrayList<>();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory).withObserver(observations::add);

        StepVerifier.create(executor.rowsUpdated(new SqlRequest("update broken set name = ?",
                                                                List.of("王"))))
                    .expectError(RdbException.class)
                    .verify();

        assertEquals(1, observations.size());
        SqlExecutionObservation observation = observations.getFirst();
        assertEquals(SqlExecutionOperation.UPDATE, observation.operation());
        assertEquals(SqlStatementType.UPDATE, observation.statementType());
        assertEquals(SqlExecutionStatus.ERROR, observation.status());
        assertEquals(SqlFailureCategory.BAD_SQL, observation.failureCategory());
        assertEquals(SqlExecutionResultKind.BAD_SQL, observation.resultKind());
        assertEquals(1, observation.parameterCount());
        assertEquals(0, observation.rows());
    }

    /** R2DBC Publisher 直接发出的 VM Error 必须穿过 onErrorMap，不能被错误分类降级为 UNKNOWN。 */
    @Test
    void preservesVirtualMachineErrorFromR2dbcPublisher() {
        OutOfMemoryError fatal = new OutOfMemoryError("driver execution failed");
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 0, "recording", fatal);
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(recordingInvalidator(reusableCloses, invalidations));

        assertInstanceOf(RdbException.class, RdbExceptionTranslator.translate(fatal));

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class, () -> executor.rowsUpdated(
                new SqlRequest("update users set status = ?", List.of("disabled"))).block());

        assertSame(fatal, error);
        assertEquals(0, reusableCloses.get());
        assertEquals(1, invalidations.get());
    }

    /** 外部事务中的 work 总超时仍必须执行会话 cleanup，不能把 lock-timeout 状态留给后续 SQL。 */
    @Test
    void totalTimeoutRunsSequenceCleanupInsideExternalTransaction() {
        String workSql = "alter table users add column email varchar(255)";
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 0, "PostgreSQL", null, Duration.ZERO, workSql);
        Connection external = factory.connection();
        AtomicInteger transactionLookups = new AtomicInteger();
        SqlExecutionObserver observer = new SqlExecutionObserver() {
            @Override
            public boolean requiresTransactionSource() {
                return true;
            }

            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // 本测试只验证事务感知观测不会让已过期的业务期限阻断 cleanup。
            }
        };
        ConnectionScopedReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withTransactionParticipant(() -> {
                    transactionLookups.incrementAndGet();
                    return Mono.just(R2dbcTransactionContext.external(external, "primary"));
                })
                .withObserver(observer);
        SqlExecutionSequence sequence = new SqlExecutionSequence(
                List.of(new SqlRequest("set lock_timeout = '1500ms'", List.of())),
                List.of(new SqlRequest(workSql, List.of())),
                List.of(new SqlRequest("reset lock_timeout", List.of())));
        SqlExecutionOptions options = SqlExecutionOptions.timeout(Duration.ofMillis(50))
                                                         .withCleanupTimeout(Duration.ofMillis(100));

        StepVerifier.withVirtualTime(() -> executor.executeInConnection(sequence, options))
                    .thenAwait(Duration.ofMillis(50))
                    .expectError(SqlExecutionTimeoutException.class)
                    .verify(Duration.ofSeconds(1));

        assertEquals(List.of("set lock_timeout = '1500ms'", workSql, "reset lock_timeout"),
                     factory.sqlHistory());
        assertEquals(1, transactionLookups.get());
        assertEquals(0, factory.closedCount());
    }

    /** 同连接序列的 setup、work、cleanup 必须复用首次解析的外部事务事实。 */
    @Test
    void resolvesExternalTransactionOnlyOnceForObservedSequence() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1, "PostgreSQL");
        Connection external = factory.connection();
        AtomicInteger transactionLookups = new AtomicInteger();
        SqlExecutionObserver observer = new SqlExecutionObserver() {
            @Override
            public boolean requiresTransactionSource() {
                return true;
            }

            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // 本测试只验证整个 Sequence 复用同一事务解析结果。
            }
        };
        ConnectionScopedReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withTransactionParticipant(() -> {
                    transactionLookups.incrementAndGet();
                    return Mono.just(R2dbcTransactionContext.external(external, "primary"));
                })
                .withObserver(observer);
        SqlExecutionSequence sequence = new SqlExecutionSequence(
                List.of(new SqlRequest("set lock_timeout = '1500ms'", List.of())),
                List.of(new SqlRequest("alter table users add column email varchar(255)", List.of())),
                List.of(new SqlRequest("reset lock_timeout", List.of())));

        StepVerifier.create(executor.executeInConnection(sequence, SqlExecutionOptions.safeDefaults()))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals(1, transactionLookups.get());
        assertEquals(List.of("set lock_timeout = '1500ms'",
                             "alter table users add column email varchar(255)",
                             "reset lock_timeout"),
                     factory.sqlHistory());
    }

    /** 同连接序列会先完成 usingWhen 清理，再在公共入口恢复被阶段异常封装的 VM Error。 */
    @Test
    void preservesVirtualMachineErrorFromR2dbcSequenceAfterCleanup() {
        OutOfMemoryError fatal = new OutOfMemoryError("sequence driver execution failed");
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 0, "recording", fatal);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(recordingInvalidator(reusableCloses, invalidations));
        SqlExecutionSequence sequence = new SqlExecutionSequence(
                List.of(),
                List.of(new SqlRequest("update users set status = ?", List.of("disabled"))),
                List.of());

        OutOfMemoryError error = assertThrows(
                OutOfMemoryError.class,
                () -> executor.executeInConnection(sequence, SqlExecutionOptions.unlimited()).block());

        assertSame(fatal, error);
        assertEquals(0, reusableCloses.get());
        assertEquals(1, invalidations.get());
    }

    /** 已确认更新的普通 close 失败后，invalidate 的 fatal 必须在观测完成后取代成功结果。 */
    @Test
    void propagatesCleanupFatalAfterConfirmedUpdateCloseFailure() {
        AtomicInteger closeAttempts = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        OutOfMemoryError fatal = new OutOfMemoryError("invalidate after close failure");
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(new RecordingConnectionFactory(List.of(), 1))
                .withConnectionInvalidator(cleanupFailingInvalidator(
                        closeAttempts,
                        invalidations,
                        new IllegalStateException("connection close failed"),
                        fatal))
                .withObserver(cleanupObserver(cleanupObservations));

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> executor.rowsUpdated(new SqlRequest("update users set status = ?", List.of("disabled")))
                              .block());

        assertSame(fatal, observed);
        assertEquals(1, closeAttempts.get());
        assertEquals(1, invalidations.get());
        assertEquals(1, cleanupObservations.size());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                     cleanupObservations.getFirst().phase());
    }

    /** 已确认更新的 close 自身发出 fatal 时，即使 invalidate 成功也不能继续返回业务成功。 */
    @Test
    void propagatesCloseFatalAfterConfirmedUpdateWhenInvalidationSucceeds() {
        AtomicInteger closeAttempts = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        OutOfMemoryError fatal = new OutOfMemoryError("close fatal after confirmed update");
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(new RecordingConnectionFactory(List.of(), 1))
                .withConnectionInvalidator(cleanupFailingInvalidator(
                        closeAttempts, invalidations, fatal, null))
                .withObserver(cleanupObserver(cleanupObservations));

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> executor.rowsUpdated(new SqlRequest("update users set status = ?", List.of("disabled")))
                              .block());

        assertSame(fatal, observed);
        assertEquals(1, closeAttempts.get());
        assertEquals(1, invalidations.get());
        assertEquals(1, cleanupObservations.size());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                     cleanupObservations.getFirst().phase());
    }

    /** 业务和 invalidate 同时发出 fatal 时，业务 fatal 仍为主，且 Throwable 图不能反向成环。 */
    @Test
    void preservesOperationFatalWhenInvalidationAlsoFailsFatally() {
        AtomicInteger closeAttempts = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        OutOfMemoryError operationFatal = new OutOfMemoryError("operation fatal");
        OutOfMemoryError invalidationFatal = new OutOfMemoryError("invalidation fatal");
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(
                        new RecordingConnectionFactory(List.of(), 0, "recording", operationFatal))
                .withConnectionInvalidator(cleanupFailingInvalidator(
                        closeAttempts, invalidations, null, invalidationFatal))
                .withObserver(cleanupObserver(cleanupObservations));

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> executor.rowsUpdated(new SqlRequest("update users set status = ?", List.of("disabled")))
                              .block());

        assertSame(operationFatal, observed);
        assertEquals(0, closeAttempts.get());
        assertEquals(1, invalidations.get());
        assertEquals(1, cleanupObservations.size());
        assertFalse(reaches(invalidationFatal, operationFatal));
    }

    @Test
    void invalidatesOwnedConnectionAfterExecutionError() {
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 0, "recording", new IllegalStateException("driver failed"));
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(recordingInvalidator(reusableCloses, invalidations));

        StepVerifier.create(executor.rowsUpdated(new SqlRequest("update broken set name = ?", List.of("王"))))
                    .expectError()
                    .verify();

        assertEquals(0, reusableCloses.get());
        assertEquals(0, factory.closedCount());
        assertEquals(1, invalidations.get());
    }

    /** 数据库明确拒绝单条 SQL 时连接会话仍可复用，不能因默认物理失效器 fail-closed 而永久占住池引用。 */
    @Test
    void closesReusableConnectionAfterClassifiedSqlError() {
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        R2dbcBadGrammarException badSql = new R2dbcBadGrammarException(
                "table or view does not exist", "42000", 942);
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 0, "Oracle", badSql);
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(recordingInvalidator(reusableCloses, invalidations));

        StepVerifier.create(executor.rowsUpdated(new SqlRequest("drop table missing_table", List.of())))
                    .expectError(RdbException.class)
                    .verify();

        assertEquals(1, reusableCloses.get());
        assertEquals(1, factory.closedCount());
        assertEquals(0, invalidations.get());
    }

    @Test
    void invalidatesOwnedConnectionAfterCancellation() {
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        String sql = "update waiting set name = ?";
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 0, "recording", null, Duration.ZERO, sql);
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(recordingInvalidator(reusableCloses, invalidations));

        StepVerifier.create(executor.rowsUpdated(new SqlRequest(sql, List.of("王"))))
                    .thenAwait(Duration.ofMillis(10))
                    .thenCancel()
                    .verify();

        assertEquals(0, reusableCloses.get());
        assertEquals(0, factory.closedCount());
        assertEquals(1, invalidations.get());
    }

    /** 查询取消交给驱动，LOB 清理完成后正常归还连接池。 */
    @Test
    void returnsOwnedConnectionAfterQueryCancellation() {
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(row("id", "u1"), row("id", "u2")), 0);
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(recordingInvalidator(reusableCloses, invalidations));

        StepVerifier.create(executor.query(new SqlRequest("select id from users", List.of())), 1)
                    .expectNextCount(1)
                    .thenCancel()
                    .verify();

        assertEquals(1, reusableCloses.get());
        assertEquals(0, invalidations.get());
    }

    @Test
    void protectsQueryFromTooManyRowsAndObservesRowLimit() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(row("id", "u1"),
                                                                                    row("id", "u2")), 0);
        List<SqlExecutionObservation> observations = new ArrayList<>();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory).withObserver(observations::add);

        StepVerifier.create(executor.query(new SqlRequest("select id from Users", List.of()),
                                           SqlExecutionOptions.maxRows(1)))
                    .expectNext(row("id", "u1"))
                    .expectError(SqlRowLimitExceededException.class)
                    .verify();

        assertEquals(1, observations.size());
        SqlExecutionObservation observation = observations.getFirst();
        assertEquals(SqlExecutionStatus.ERROR, observation.status());
        assertEquals(SqlFailureCategory.ROW_LIMIT, observation.failureCategory());
        assertEquals(SqlExecutionResultKind.ROW_LIMIT, observation.resultKind());
        assertEquals(1, observation.rows());
    }

    @Test
    void protectsQueryFromExceedingTotalResultMemory() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(row("name", "1234567890"), row("name", "abcdefghij")), 0);
        List<SqlExecutionObservation> observations = new ArrayList<>();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory).withObserver(observations::add);
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults().withMaxResultBytes(100);

        StepVerifier.create(executor.query(new SqlRequest("select name from Users", List.of()), options))
                    .expectNext(row("name", "1234567890"))
                    .expectError(SqlResultMemoryLimitExceededException.class)
                    .verify();

        assertEquals(SqlFailureCategory.RESULT_MEMORY_LIMIT,
                     observations.getFirst().failureCategory());
        assertEquals(SqlExecutionResultKind.RESULT_MEMORY_LIMIT,
                     observations.getFirst().resultKind());
        assertEquals(1, observations.getFirst().rows());
    }

    /**
     * 自定义 ReactiveSqlExecutor 也可以用接口默认方法包一层观测。
     */
    @Test
    void wrapsCustomReactiveExecutorWithObserver() {
        List<SqlExecutionObservation> observations = new ArrayList<>();
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.just(row("id", "u1"));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(3L);
            }
        };

        ReactiveSqlExecutor executor = delegate.withObserver(observations::add);

        StepVerifier.create(executor.rowsUpdated(new SqlRequest("delete from Users where status = ?",
                                                                List.of("disabled"))))
                    .expectNext(3L)
                    .verifyComplete();

        assertEquals(1, observations.size());
        SqlExecutionObservation observation = observations.getFirst();
        assertEquals(SqlExecutionOperation.UPDATE, observation.operation());
        assertEquals(SqlStatementType.DELETE, observation.statementType());
        assertEquals(SqlExecutionStatus.SUCCESS, observation.status());
        assertEquals(3L, observation.rows());
    }

    /** 自定义执行器的未知阶段可能包含连接池排队，接口默认方法不得越权给整个 Publisher 计时。 */
    @Test
    void defaultWriteDoesNotTimeOpaqueCustomExecutorPhase() {
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.never();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.delay(Duration.ofMillis(20)).thenReturn(1L);
            }
        };

        StepVerifier.withVirtualTime(() -> delegate.rowsUpdated(
                            new SqlRequest("update Users set name = ?", List.of("王")),
                            SqlExecutionOptions.timeout(Duration.ofMillis(10))))
                    .thenAwait(Duration.ofMillis(20))
                    .expectNext(1L)
                    .verifyComplete();
    }

    /** 查询默认方法保留结果保护，但不能把 SQL timeout 套在自定义执行器不可分阶段的 Publisher 外。 */
    @Test
    void defaultQueryDoesNotTimeOpaqueCustomExecutorPhase() {
        DynamicRow first = row("id", "u1");
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Mono.delay(Duration.ofMillis(20)).thenMany(Flux.just(first));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(0L);
            }
        };

        StepVerifier.withVirtualTime(() -> delegate.query(
                            new SqlRequest("select id from Users", List.of()),
                            SqlExecutionOptions.timeout(Duration.ofMillis(10))))
                    .thenAwait(Duration.ofMillis(20))
                    .expectNext(first)
                    .verifyComplete();
    }

    /**
     * 查询超时是整条 SQL 的总时限。即使结果每隔一小段时间就在返回，也不能无限续期。
     */
    @Test
    void queryTimeoutUsesOneTotalDeadlineInsteadOfIdleGaps() {
        DynamicRow first = row("id", "u1");
        DynamicRow second = row("id", "u2");
        DynamicRow third = row("id", "u3");
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(first, second, third),
                                                                              0,
                                                                              "recording",
                                                                              null,
                                                                              Duration.ofSeconds(1));
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        StepVerifier.withVirtualTime(() -> executor.query(new SqlRequest("select id from Users", List.of()),
                                                           SqlExecutionOptions.timeout(
                                                                   Duration.ofMillis(2500))))
                    .thenAwait(Duration.ofSeconds(3))
                    .expectNext(first, second)
                    .expectError(SqlExecutionTimeoutException.class)
                    .verify();
    }

    /** 普通 SQL 的连接排队与获取超时由上层连接池负责。 */
    @Test
    void delegatesOrdinaryConnectionWaitingToThePool() {
        ConnectionFactory factory = neverConnectionFactory();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);
        SqlExecutionOptions options = SqlExecutionOptions.unlimited();

        StepVerifier.withVirtualTime(() -> executor.query(
                            new SqlRequest("select id from Users", List.of()), options))
                    .thenAwait(Duration.ofSeconds(1))
                    .thenCancel()
                    .verify();
    }

    /** 外部事务查找由上层事务管理器控制，ORM 的 SQL 时限不能越权终止它。 */
    @Test
    void delegatesTransactionResolutionWaitingToTheUpperLayer() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        AtomicInteger transactionLookups = new AtomicInteger();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory).withTransactionParticipant(() -> {
            transactionLookups.incrementAndGet();
            return Mono.never();
        });
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                                                         .withTimeout(Duration.ofMillis(10));

        StepVerifier.withVirtualTime(() -> executor.rowsUpdated(
                            new SqlRequest("update Users set enabled = ?", List.of(true)), options))
                    .thenAwait(Duration.ofSeconds(1))
                    .thenCancel()
                    .verify();

        assertEquals(1, transactionLookups.get());
        assertEquals(0, factory.createdCount());
    }

    /** SQL 总时限从连接可用后开始，不能把上层事务查找耗时算进数据库执行。 */
    @Test
    void startsSqlTimeoutAfterTransactionResolutionAndConnectionAcquisition() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(row("id", "u1")), 0L, "recording", null, Duration.ofMillis(60));
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withTransactionParticipant(() -> Mono.delay(Duration.ofMillis(60)).then(Mono.empty()));
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                                                         .withTimeout(Duration.ofMillis(100));

        StepVerifier.withVirtualTime(() -> executor.query(
                            new SqlRequest("select id from Users", List.of()), options).collectList())
                    .thenAwait(Duration.ofMillis(121))
                    .assertNext(rows -> assertEquals(1, rows.size()))
                    .verifyComplete();
    }

    /** 同连接 SQL 序列也必须在事务解析和连接获取完成后才启动 SQL 时限。 */
    @Test
    void startsSequenceTimeoutAfterTransactionResolutionAndConnectionAcquisition() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(), 1L, "recording", null, Duration.ofMillis(60));
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withTransactionParticipant(() -> Mono.delay(Duration.ofMillis(60)).then(Mono.empty()));
        SqlExecutionSequence sequence = new SqlExecutionSequence(
                List.of(),
                List.of(new SqlRequest("update Users set enabled = true", List.of())),
                List.of());
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                                                         .withTimeout(Duration.ofMillis(100));

        StepVerifier.withVirtualTime(() -> executor.executeInConnection(sequence, options))
                    .thenAwait(Duration.ofMillis(121))
                    .assertNext(result -> assertEquals(1L, result.rowsUpdated()))
                    .verifyComplete();
    }

    /** 批量读取外部事务时服从上层事务管理器，ORM 的 SQL 时限不能取消事务解析。 */
    @Test
    void delegatesBatchTransactionResolutionToTheUpperTransactionManager() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(List.of(), 1);
        AtomicInteger transactionLookups = new AtomicInteger();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory).withTransactionParticipant(() -> {
            transactionLookups.incrementAndGet();
            return Mono.never();
        });
        BatchWriteOptions options = BatchWriteOptions.atomic(1)
                                                     .withTimeout(Duration.ofMillis(10));
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users(id) values(?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Mono.just(new Object[]{"u1"}),
                options);

        StepVerifier.withVirtualTime(() -> executor.writeBatch(request))
                    .thenAwait(Duration.ofSeconds(1))
                    .thenCancel()
                    .verify();

        assertTrue(transactionLookups.get() <= 1);
        assertEquals(0, factory.createdCount());
    }

    /** ATOMIC 批量连接排队完全服从连接池，ORM 的批量 SQL 时限不能取消获取连接。 */
    @Test
    void delegatesAtomicBatchConnectionWaitingToThePool() {
        ConnectionFactory factory = neverConnectionFactory();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);
        BatchWriteOptions options = BatchWriteOptions.atomic(100)
                                                     .withTimeout(Duration.ofMillis(10));
        BatchWriteRequest request = new BatchWriteRequest("insert into Users(id) values(?)",
                                                          1,
                                                          List.of(String.class),
                                                          SqlBindMarkerStyle.CANONICAL,
                                                          Mono.just(new Object[]{"u1"}),
                                                          options);

        StepVerifier.withVirtualTime(() -> executor.writeBatch(request))
                    .thenAwait(Duration.ofSeconds(1))
                    .thenCancel()
                    .verify();
    }

    /**
     * 回执查询报错和“没有回执”是两回事。查询失败必须抛出，不能静默伪装成 UNKNOWN。
     */
    @Test
    void propagatesReceiptLookupFailureWhenResolvingUnknown() {
        ConnectionFactory factory = failingConnectionFactory();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);
        BatchChunkResult.RecoveryToken token = new BatchChunkResult.RecoveryToken("operation-1",
                                                                                   0,
                                                                                   "flying_batch_receipt",
                                                                                   "plan",
                                                                                   "payload",
                                                                                   1L,
                                                                                   1L);

        StepVerifier.create(executor.resolveUnknown(token))
                    .expectError()
                    .verify();
    }

    /** R2DBC 查询必须把执行选项中的有界预取提示交给驱动 Statement。 */
    @Test
    void appliesConfiguredFetchSizeToR2dbcStatement() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(Map.of("id", "u1")), 0, "recording");
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        StepVerifier.create(executor.query(new SqlRequest("select id from Users", List.of()),
                                           SqlExecutionOptions.safeDefaults().withFetchSize(37)))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals(37, factory.fetchSize);
    }

    /** 普通查询应保留驱动默认抓取策略，避免小结果集被无差别切换到游标协议。 */
    @Test
    void keepsDriverFetchStrategyWhenFetchSizeIsNotExplicit() {
        RecordingConnectionFactory factory = new RecordingConnectionFactory(
                List.of(Map.of("id", "u1")), 0, "recording");
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        StepVerifier.create(executor.query(new SqlRequest("select id from Users", List.of())))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals(0, factory.fetchSize);
    }

    /** 回执读取资源域保留的 cleanup wrapper 必须在 resolveUnknown 公共入口恢复为原 fatal。 */
    @Test
    void resolveUnknownRestoresFatalFromReceiptCleanupWrapper() {
        AtomicInteger closeAttempts = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        OutOfMemoryError fatal = new OutOfMemoryError("receipt invalidation fatal");
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(new RecordingConnectionFactory(
                List.of(), 0, "recording", new IllegalStateException("receipt query failed")))
                .withConnectionInvalidator(cleanupFailingInvalidator(
                        closeAttempts, invalidations, null, fatal));
        BatchChunkResult.RecoveryToken token = new BatchChunkResult.RecoveryToken("operation-1",
                                                                                   0,
                                                                                   "flying_batch_receipt",
                                                                                   "plan",
                                                                                   "payload",
                                                                                   1L,
                                                                                   1L);

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> executor.resolveUnknown(token).block());

        assertSame(fatal, observed);
        assertEquals(0, closeAttempts.get());
        assertEquals(1, invalidations.get());
    }

    @Test
    void incompleteStreamingRecoveryEvidenceRemainsUnknownWithoutTrustingOperationId() {
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(failingConnectionFactory());
        BatchChunkResult.RecoveryToken token = new BatchChunkResult.RecoveryToken("operation-1",
                                                                                   0,
                                                                                   "flying_batch_receipt",
                                                                                   "plan",
                                                                                   null,
                                                                                   null,
                                                                                   null);

        StepVerifier.create(executor.resolveUnknown(token))
                    .assertNext(resolution -> assertEquals(BatchResolution.Status.UNKNOWN, resolution.status()))
                    .verifyComplete();
    }

    private static ConnectionFactory neverConnectionFactory() {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.never();
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "starved-pool";
            }
        };
    }

    private static ConnectionFactory failingConnectionFactory() {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.error(new IllegalStateException("receipt connection failed"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "failing-receipt";
            }
        };
    }

    private static SqlExecutionObserver cleanupObserver(List<ResourceCleanupObservation> observations) {
        return new SqlExecutionObserver() {
            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // 聚焦验证资源清理事实，不收集普通 SQL 观测。
            }

            @Override
            public void onResourceCleanup(ResourceCleanupObservation observation) {
                observations.add(observation);
            }
        };
    }

    private static R2dbcConnectionInvalidator recordingInvalidator(AtomicInteger reusableCloses,
                                                                    AtomicInteger invalidations) {
        return R2dbcConnectionInvalidator.of(connection -> {
            reusableCloses.incrementAndGet();
            return connection.close();
        }, connection -> {
            invalidations.incrementAndGet();
            return Mono.empty();
        });
    }

    private static DynamicRow row(Object... values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            row.put((String) values[i], values[i + 1]);
        }
        return DynamicRow.copyOf(row);
    }

    private sealed interface RecordedBind permits ValueBind, NullBind {
    }

    private record ValueBind(int index, Object value) implements RecordedBind {
    }

    private record NullBind(int index, Class<?> type) implements RecordedBind {
    }

    private static final class RecordingConnectionFactory implements ConnectionFactory {

        private final List<Map<String, Object>> rows;
        private final long rowsUpdated;
        private final List<Long> resultUpdateCounts;
        private final List<RecordedBind> binds = new ArrayList<>();
        private final AtomicInteger addCount = new AtomicInteger();
        private final AtomicInteger closedCount = new AtomicInteger();
        private final AtomicInteger createdCount = new AtomicInteger();
        private final AtomicInteger beginCount = new AtomicInteger();
        private final AtomicInteger commitCount = new AtomicInteger();
        private final AtomicInteger rollbackCount = new AtomicInteger();
        private final List<String> sqlHistory = new ArrayList<>();
        private final List<String> generatedKeyColumns = new ArrayList<>();
        private final String databaseName;
        private final Throwable executeError;
        private final Duration rowDelay;
        private final String neverSql;
        private final RuntimeException closeError;
        private final Duration closeDelay;
        private final boolean closeNever;
        private boolean beginNever;
        private boolean commitNever;
        private Throwable rollbackError;
        private Duration rollbackDelay = Duration.ZERO;
        private String zeroRowsSql;
        private String sql;
        private int fetchSize;

        private RecordingConnectionFactory(List<Map<String, Object>> rows, long rowsUpdated) {
            this(rows, rowsUpdated, "recording");
        }

        private RecordingConnectionFactory(List<Map<String, Object>> rows, List<Long> resultUpdateCounts) {
            this.rows = rows;
            this.rowsUpdated = 0L;
            this.resultUpdateCounts = List.copyOf(resultUpdateCounts);
            this.databaseName = "recording";
            this.executeError = null;
            this.rowDelay = Duration.ZERO;
            this.neverSql = null;
            this.closeError = null;
            this.closeDelay = Duration.ZERO;
            this.closeNever = false;
        }

        private RecordingConnectionFactory(List<Map<String, Object>> rows, long rowsUpdated, String databaseName) {
            this(rows, rowsUpdated, databaseName, null);
        }

        private RecordingConnectionFactory(List<Map<String, Object>> rows,
                                           long rowsUpdated,
                                           String databaseName,
                                           Throwable executeError) {
            this(rows, rowsUpdated, databaseName, executeError, Duration.ZERO);
        }

        private RecordingConnectionFactory(List<Map<String, Object>> rows,
                                           long rowsUpdated,
                                           String databaseName,
                                           Throwable executeError,
                                           Duration rowDelay) {
            this(rows, rowsUpdated, databaseName, executeError, rowDelay, null);
        }

        private RecordingConnectionFactory(List<Map<String, Object>> rows,
                                            long rowsUpdated,
                                            String databaseName,
                                            Throwable executeError,
                                            Duration rowDelay,
                                            String neverSql) {
            this(rows, rowsUpdated, databaseName, executeError, rowDelay, neverSql, null);
        }

        private RecordingConnectionFactory(List<Map<String, Object>> rows,
                                            long rowsUpdated,
                                            String databaseName,
                                            Throwable executeError,
                                            Duration rowDelay,
                                            String neverSql,
                                            RuntimeException closeError) {
            this(rows,
                 rowsUpdated,
                 databaseName,
                 executeError,
                 rowDelay,
                 neverSql,
                 closeError,
                 Duration.ZERO,
                 false);
        }

        private RecordingConnectionFactory(List<Map<String, Object>> rows,
                                            long rowsUpdated,
                                            String databaseName,
                                            Throwable executeError,
                                            Duration rowDelay,
                                            String neverSql,
                                            RuntimeException closeError,
                                            Duration closeDelay,
                                            boolean closeNever) {
            this.rows = rows;
            this.rowsUpdated = rowsUpdated;
            this.resultUpdateCounts = List.of(rowsUpdated);
            this.databaseName = databaseName;
            this.executeError = executeError;
            this.rowDelay = rowDelay;
            this.neverSql = neverSql;
            this.closeError = closeError;
            this.closeDelay = closeDelay;
            this.closeNever = closeNever;
        }

        @Override
        public Publisher<? extends Connection> create() {
            createdCount.incrementAndGet();
            return Mono.just(connection());
        }

        @Override
        public ConnectionFactoryMetadata getMetadata() {
            return () -> databaseName;
        }

        private String sql() {
            return sql;
        }

        private List<RecordedBind> binds() {
            return binds;
        }

        private int closedCount() {
            return closedCount.get();
        }

        private int createdCount() {
            return createdCount.get();
        }

        private List<String> sqlHistory() {
            return List.copyOf(sqlHistory);
        }

        private List<String> generatedKeyColumns() {
            return List.copyOf(generatedKeyColumns);
        }

        private int addCount() {
            return addCount.get();
        }

        private int beginCount() {
            return beginCount.get();
        }

        private int commitCount() {
            return commitCount.get();
        }

        private int rollbackCount() {
            return rollbackCount.get();
        }

        private RecordingConnectionFactory hangCommit() {
            this.commitNever = true;
            return this;
        }

        private RecordingConnectionFactory hangBegin() {
            this.beginNever = true;
            return this;
        }

        private RecordingConnectionFactory failRollback(Throwable error) {
            this.rollbackError = Objects.requireNonNull(error, "rollback error must not be null");
            return this;
        }

        private RecordingConnectionFactory delayRollback(Duration delay) {
            this.rollbackDelay = Objects.requireNonNull(delay, "rollback delay must not be null");
            return this;
        }

        private RecordingConnectionFactory zeroRowsFor(String targetSql) {
            this.zeroRowsSql = targetSql;
            return this;
        }

        private Connection connection() {
            return proxy(Connection.class, (proxy, method, args) -> switch (method.getName()) {
                case "createStatement" -> {
                    sql = (String) args[0];
                    sqlHistory.add(sql);
                    yield statement();
                }
                case "close" -> {
                    closedCount.incrementAndGet();
                    if (closeNever) {
                        yield Mono.never();
                    }
                    if (closeError != null) {
                        yield Mono.error(closeError);
                    }
                    yield closeDelay.isZero() ? Mono.empty() : Mono.delay(closeDelay).then();
                }
                case "beginTransaction" -> {
                    beginCount.incrementAndGet();
                    yield beginNever ? Mono.never() : Mono.empty();
                }
                case "commitTransaction" -> {
                    commitCount.incrementAndGet();
                    yield commitNever ? Mono.never() : Mono.empty();
                }
                case "rollbackTransaction" -> {
                    rollbackCount.incrementAndGet();
                    if (rollbackError != null) {
                        yield rawError(rollbackError);
                    }
                    yield rollbackDelay.isZero() ? Mono.empty() : Mono.delay(rollbackDelay).then();
                }
                case "validate" -> Mono.just(true);
                case "isAutoCommit" -> true;
                case "getMetadata" -> proxy(ConnectionMetadata.class, (ignored, metadataMethod, ignoredArgs) -> {
                    if ("getName".equals(metadataMethod.getName())) {
                        return databaseName;
                    }
                    return defaultValue(metadataMethod);
                });
                default -> defaultValue(method);
            });
        }

        private Statement statement() {
            RecordingStatement handler = new RecordingStatement();
            Statement statement = proxy(Statement.class, handler);
            handler.statement = statement;
            return statement;
        }

        private Result result(long updateCount) {
            return proxy(Result.class, (proxy, method, args) -> switch (method.getName()) {
                case "flatMap" -> flatMapSegments(updateCount, args);
                case "map" -> mapRows(args);
                case "getRowsUpdated" -> Mono.just(updateCount);
                default -> defaultValue(method);
            });
        }

        @SuppressWarnings("unchecked")
        private Publisher<?> flatMapSegments(long updateCount, Object[] args) {
            Function<Result.Segment, ? extends Publisher<?>> mapper =
                    (Function<Result.Segment, ? extends Publisher<?>>) args[0];
            Result.Segment count = proxy(Result.UpdateCount.class, (ignored, method, ignoredArgs) ->
                    "value".equals(method.getName()) ? updateCount : defaultValue(method));
            Flux<Result.Segment> rowSegments = Flux.fromIterable(rows)
                    .map(values -> proxy(Result.RowSegment.class,
                            (ignored, method, ignoredArgs) -> "row".equals(method.getName())
                                    ? row(values) : defaultValue(method)));
            if (!rowDelay.isZero()) {
                rowSegments = rowSegments.delayElements(rowDelay);
            }
            return Flux.concat(Flux.just(count), rowSegments)
                       .concatMap(segment -> Flux.from(mapper.apply(segment)));
        }

        @SuppressWarnings("unchecked")
        private Publisher<?> mapRows(Object[] args) {
            BiFunction<Row, RowMetadata, ?> mapper = (BiFunction<Row, RowMetadata, ?>) args[0];
            RowMetadata metadata = rowMetadata();
            Flux<?> mapped = Flux.fromIterable(rows).map(row -> mapper.apply(row(row), metadata));
            return rowDelay.isZero() ? mapped : mapped.delayElements(rowDelay);
        }

        private Row row(Map<String, Object> values) {
            List<String> names = new ArrayList<>(values.keySet());
            return proxy(Row.class, (proxy, method, args) -> {
                if ("getMetadata".equals(method.getName())) {
                    return rowMetadata();
                }
                if ("get".equals(method.getName()) && args[0] instanceof String name) {
                    return values.get(name);
                }
                if ("get".equals(method.getName()) && args[0] instanceof Integer index) {
                    return values.get(names.get(index));
                }
                return defaultValue(method);
            });
        }

        private RowMetadata rowMetadata() {
            List<ColumnMetadata> columns = rows.isEmpty()
                    ? List.of()
                    : rows.get(0).keySet().stream().map(this::column).toList();
            return proxy(RowMetadata.class, (proxy, method, args) -> {
                if ("getColumnMetadatas".equals(method.getName())) {
                    return columns;
                }
                return defaultValue(method);
            });
        }

        private ColumnMetadata column(String name) {
            return proxy(ColumnMetadata.class, (proxy, method, args) -> {
                if ("getName".equals(method.getName())) {
                    return name;
                }
                return defaultValue(method);
            });
        }

        private final class RecordingStatement implements InvocationHandler {

            private Statement statement;

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) {
                return switch (method.getName()) {
                    case "bind" -> {
                        binds.add(new ValueBind((Integer) args[0], args[1]));
                        yield statement;
                    }
                    case "bindNull" -> {
                        binds.add(new NullBind((Integer) args[0], (Class<?>) args[1]));
                        yield statement;
                    }
                    case "execute" -> {
                        if (neverSql != null && neverSql.equals(sql)) {
                            yield Flux.never();
                        }
                        List<Long> updateCounts = Objects.equals(zeroRowsSql, sql)
                                ? List.of(0L) : resultUpdateCounts;
                        yield executeError == null
                                ? Flux.fromIterable(updateCounts).map(RecordingConnectionFactory.this::result)
                                : executeError instanceof VirtualMachineError
                                        ? rawError(executeError)
                                        : Flux.error(executeError);
                    }
                    case "add" -> {
                        addCount.incrementAndGet();
                        yield statement;
                    }
                    case "fetchSize" -> {
                        fetchSize = (Integer) args[0];
                        yield statement;
                    }
                    case "returnGeneratedValues" -> {
                        if (args != null && args.length == 1 && args[0] instanceof String[] columns) {
                            generatedKeyColumns.addAll(List.of(columns));
                        }
                        yield statement;
                    }
                    default -> defaultValue(method);
                };
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    /**
     * 直接发出 onError，避免 Flux.error 对 JVM fatal 的内部快捷路径遮蔽 executor 的 onErrorMap 契约。
     */
    private static <T> Publisher<T> rawError(Throwable error) {
        return subscriber -> subscriber.onSubscribe(new Subscription() {
            private boolean terminated;

            @Override
            public void request(long demand) {
                if (!terminated) {
                    terminated = true;
                    subscriber.onError(error);
                }
            }

            @Override
            public void cancel() {
                terminated = true;
            }
        });
    }

    private static R2dbcConnectionInvalidator cleanupFailingInvalidator(AtomicInteger closeAttempts,
                                                                          AtomicInteger invalidations,
                                                                          Throwable closeFailure,
                                                                          Throwable invalidationFailure) {
        return R2dbcConnectionInvalidator.of(connection -> {
            closeAttempts.incrementAndGet();
            return closeFailure == null ? connection.close() : rawError(closeFailure);
        }, connection -> {
            invalidations.incrementAndGet();
            return invalidationFailure == null ? Mono.empty() : rawError(invalidationFailure);
        });
    }

    private static boolean reaches(Throwable start, Throwable expected) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(start);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == expected) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addLast(cause);
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return false;
    }

    private static Object defaultValue(Method method) {
        Class<?> returnType = method.getReturnType();
        if (method.getDeclaringClass() == Object.class) {
            return switch (method.getName()) {
                case "toString" -> method.getDeclaringClass().getName();
                case "hashCode" -> 0;
                case "equals" -> false;
                default -> null;
            };
        }
        if (Publisher.class.isAssignableFrom(returnType)) {
            return Mono.empty();
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        return null;
    }
}
