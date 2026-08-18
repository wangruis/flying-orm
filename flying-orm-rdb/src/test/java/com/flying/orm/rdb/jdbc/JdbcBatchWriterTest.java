package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.BatchExecutionEventType;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.JdbcTransactionCompletion;
import com.flying.orm.rdb.transaction.TransactionOutcome;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 原生 JDBC 批量的最小事务与资源边界回归测试。 */
class JdbcBatchWriterTest {

    /** 受保护批量更新必须把预读 owner 重新附加到业务 SQL，不能只用相同影响行数猜测行身份。 */
    @Test
    void restrictsProtectedBatchUpdateToTheOwnerReadBeforeTheWrite() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_protected_batch_owner_restriction");
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table protected_customer(id bigint primary key, label varchar, contact varbinary)");
            statement.execute("create table protected_customer_tokens("
                    + "id bigint not null, field_tag varchar(30) not null, token_hash varbinary not null)");
            statement.execute("insert into protected_customer(id, label, contact) values (1, 'target', X'01')");
        }
        String writeSql = "update protected_customer set contact = ? where label = ?";
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPDATE,
                new com.flying.orm.core.sql.render.SqlRequest(writeSql, List.of(new byte[]{9}, "target")),
                new com.flying.orm.core.sql.render.SqlRequest(
                        "select id from protected_customer where label = ?", List.of("target")),
                List.of("id"), java.util.Map.of(), "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{2}))));
        Object[] row = ProtectedBatchRows.extend(new Object[]{new byte[]{9}, "target"}, work);
        BatchWriteRequest request = new BatchWriteRequest(
                writeSql, 2, List.of(byte[].class, String.class), SqlBindMarkerStyle.CANONICAL,
                new TrackingPublisher(List.<Object[]>of()), BatchWriteOptions.atomic(1),
                BatchRowCountPolicy.EXACTLY_ONE);
        List<String> preparedSql = new ArrayList<>();

        try (Connection delegate = dataSource.getConnection()) {
            delegate.setAutoCommit(false);
            Connection recording = recordingConnection(delegate, preparedSql);
            BatchChunkResult result = new JdbcBatchChunkExecutor().execute(
                    recording, request, 0, 0L, List.<Object[]>of(row),
                    JdbcBatchSupport.BatchDeadline.start(Duration.ZERO));

            assertEquals(BatchChunkResult.Status.COMMITTED, result.status());
            assertTrue(preparedSql.contains(writeSql + " and ((id = ?))"));
            delegate.rollback();
        }
    }

    /** owner 游标推进期间收到线程中断后必须先取消语句，不能继续读取主键并准备业务更新。 */
    @Test
    void cancelsProtectedBatchOwnerReadWhenInterruptedWhileAdvancingResultSet() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_protected_batch_owner_interruption");
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table protected_customer(id bigint primary key, label varchar, contact varbinary)");
            statement.execute("insert into protected_customer(id, label, contact) values (1, 'target', X'01')");
        }
        String writeSql = "update protected_customer set contact = ? where label = ?";
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPDATE,
                new com.flying.orm.core.sql.render.SqlRequest(writeSql, List.of(new byte[]{9}, "target")),
                new com.flying.orm.core.sql.render.SqlRequest(
                        "select id from protected_customer where label = ?", List.of("target")),
                List.of("id"), java.util.Map.of(), "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{2}))));
        Object[] row = ProtectedBatchRows.extend(new Object[]{new byte[]{9}, "target"}, work);
        BatchWriteRequest request = new BatchWriteRequest(
                writeSql, 2, List.of(byte[].class, String.class), SqlBindMarkerStyle.CANONICAL,
                new TrackingPublisher(List.<Object[]>of()), BatchWriteOptions.atomic(1),
                BatchRowCountPolicy.EXACTLY_ONE);
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicInteger valueReads = new AtomicInteger();
        AtomicInteger cancellationCalls = new AtomicInteger();

        SQLException failure = null;
        try (Connection delegate = dataSource.getConnection()) {
            Connection connection = interruptingOwnerQueryConnection(
                    delegate, nextCalls, valueReads, cancellationCalls);
            try {
                failure = assertThrows(SQLException.class, () -> new JdbcProtectedBatchSideIndex().prepare(
                        connection, request, List.<Object[]>of(row),
                        JdbcBatchSupport.BatchDeadline.start(Duration.ZERO)));
            } finally {
                Thread.interrupted();
            }
        }

        assertEquals("HY008", failure.getSQLState());
        assertEquals(1, nextCalls.get());
        assertEquals(0, valueReads.get());
        assertEquals(1, cancellationCalls.get());
    }

    /** 受保护批量预读的 ARRAY owner 必须在结果集关闭前物化并释放驱动临时句柄。 */
    @Test
    void materializesProtectedBatchArrayOwnerBeforeClosingResultSet() throws Exception {
        AtomicBoolean freed = new AtomicBoolean();
        Array array = jdbcArray(new Long[]{1L, 2L}, freed);
        Connection connection = protectedOwnerConnection(array);
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPDATE,
                new com.flying.orm.core.sql.render.SqlRequest("update protected_record set value = 1", List.of()),
                new com.flying.orm.core.sql.render.SqlRequest("select id from protected_record", List.of()),
                List.of("id"), Map.of(), "id = ?",
                "delete from protected_record_tokens where id = ? and field_tag = ?",
                "insert into protected_record_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("value", List.of(new byte[]{1}))));
        Object[] row = ProtectedBatchRows.extend(new Object[0], work);
        BatchWriteRequest request = new BatchWriteRequest(
                work.writeRequest().sql(), 0, List.of(), SqlBindMarkerStyle.CANONICAL,
                new TrackingPublisher(List.<Object[]>of()), BatchWriteOptions.atomic(1),
                BatchRowCountPolicy.EXACTLY_ONE);

        JdbcProtectedBatchSideIndex.Prepared prepared = new JdbcProtectedBatchSideIndex().prepare(
                connection, request, List.<Object[]>of(row), JdbcBatchSupport.BatchDeadline.start(Duration.ZERO));

        Object owner = prepared.rows().getFirst().owners().getFirst().get("id");
        assertArrayEquals(new Long[]{1L, 2L}, assertInstanceOf(Long[].class, owner));
        assertTrue(freed.get());
    }

    /** 批量侧索引 INSERT 返回零影响行时必须回滚同分片业务写。 */
    @Test
    void rollsBackProtectedBatchWhenContainsTokenInsertAffectsNoRow() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_protected_batch_zero_token_insert");
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table protected_customer(id bigint primary key, contact varbinary)");
            statement.execute("create table protected_customer_tokens("
                    + "id bigint not null, field_tag varchar(30) not null, token_hash varbinary not null)");
        }
        ProtectedWriteWork valid = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new com.flying.orm.core.sql.render.SqlRequest(
                        "insert into protected_customer(id, contact) values (?, ?)",
                        List.of(1L, new byte[]{9})),
                null, List.of("id"), java.util.Map.of("id", 1L), "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) "
                        + "select ?, ?, ? where 1 = 0",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1}))));
        Object[] row = ProtectedBatchRows.extend(new Object[]{1L, new byte[]{9}}, valid);
        BatchWriteRequest request = new BatchWriteRequest(
                valid.writeRequest().sql(), 2, List.of(Long.class, byte[].class),
                SqlBindMarkerStyle.CANONICAL, new TrackingPublisher(List.<Object[]>of(row)),
                BatchWriteOptions.atomic(1), BatchRowCountPolicy.EXACTLY_ONE);

        BatchWriteException failure = assertThrows(
                BatchWriteException.class, () -> JdbcBatchWriter.create(dataSource).writeProtectedBatch(request));

        assertEquals(BatchWriteResult.Status.ROLLED_BACK, failure.result().status());
        JdbcSqlExecutor verifier = JdbcSqlExecutor.create(dataSource);
        assertEquals(0L, verifier.query(new com.flying.orm.core.sql.render.SqlRequest(
                "select id from protected_customer", List.of())).size());
    }

    @Test
    void writesGeneratedKeysOneRowAtATimeAndUsesGlobalInputOffsets() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_generated_keys");
        createTable(dataSource);
        List<Long> offsets = new ArrayList<>();
        List<Long> keys = new ArrayList<>();
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into device(name) values (?)", 1, List.of(String.class), SqlBindMarkerStyle.CANONICAL,
                new TrackingPublisher(List.of(new Object[]{"a"}, new Object[]{"b"}, new Object[]{"c"})),
                BatchWriteOptions.atomic(2), BatchRowCountPolicy.EXACTLY_ONE,
                BatchGeneratedKeys.required("id", (offset, key) -> {
                    offsets.add(offset);
                    keys.add(((Number) key.value(0)).longValue());
                }),
                BatchWriteCompletion.noop());

        BatchWriteResult result = JdbcBatchWriter.create(dataSource).writeBatch(request);

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(3L, result.affectedRows());
        assertEquals(List.of(0L, 1L, 2L), offsets);
        assertEquals(3, keys.size());
        assertEquals(3L, count(dataSource));
    }

    /** 普通 JDBC batch 必须把 null 参数交给驱动，而不是在构造参数列表时失败。 */
    @Test
    void bindsNullValuesInRegularJdbcBatch() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_null_regular");
        createNullableTable(dataSource);
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into nullable_device(note) values (?)", 1, List.of(String.class), SqlBindMarkerStyle.CANONICAL,
                new TrackingPublisher(List.of(new Object[]{null}, new Object[]{"kept"})),
                BatchWriteOptions.atomic(2));

        BatchWriteResult result = JdbcBatchWriter.create(dataSource).writeBatch(request);

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(2L, result.affectedRows());
        assertEquals(2L, nullableCount(dataSource));
    }

    /** 逐行读取生成键的 JDBC 分支也必须保留 null 参数的标准 JDBC 绑定语义。 */
    @Test
    void bindsNullValuesInGeneratedKeyJdbcBatch() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_null_generated_key");
        createNullableTable(dataSource);
        AtomicInteger generatedKeys = new AtomicInteger();
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into nullable_device(note) values (?)", 1, List.of(String.class), SqlBindMarkerStyle.CANONICAL,
                new TrackingPublisher(List.of(new Object[]{null}, new Object[]{"kept"})),
                BatchWriteOptions.atomic(2), BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.required("id", (offset, key) -> generatedKeys.incrementAndGet()),
                BatchWriteCompletion.noop());

        BatchWriteResult result = JdbcBatchWriter.create(dataSource).writeBatch(request);

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(2L, result.affectedRows());
        assertEquals(2, generatedKeys.get());
        assertEquals(2L, nullableCount(dataSource));
    }

    /** Publisher 在 onNext 返回后复用参数数组时，JDBC 批写入必须使用接收信号那一刻的值。 */
    @Test
    void snapshotsPublisherRowBeforeThePublisherCanReuseItsArray() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_row_snapshot");
        createTable(dataSource);
        Object[] sharedRow = {"before"};
        Publisher<Object[]> rows = subscriber -> subscriber.onSubscribe(new Subscription() {
            private boolean emitted;

            @Override
            public void request(long requested) {
                if (emitted || requested <= 0L) {
                    return;
                }
                emitted = true;
                subscriber.onNext(sharedRow);
                sharedRow[0] = "after";
                subscriber.onComplete();
            }

            @Override
            public void cancel() {
                // 单行同步 publisher 在发出后已完成，不需要额外资源清理。
            }
        });

        BatchWriteResult result = JdbcBatchWriter.create(dataSource).writeBatch(
                request(rows, BatchWriteOptions.atomic(1)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals("before", onlyName(dataSource));
    }

    /** 上游以非 VM 的 Error 终止时，ATOMIC 路径仍应保留原始失败作为批处理根因。 */
    @Test
    void keepsUpstreamErrorAsTheAtomicBatchFailureCause() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_upstream_error");
        createTable(dataSource);
        AssertionError expected = new AssertionError("upstream failed");
        Publisher<Object[]> rows = subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long requested) {
                subscriber.onError(expected);
            }

            @Override
            public void cancel() {
                // 上游已同步终止，无需额外资源回收。
            }
        });

        BatchWriteException error = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(dataSource).writeBatch(request(rows, BatchWriteOptions.atomic(1))));

        assertSame(expected, error.getCause());
        assertEquals(BatchWriteResult.Status.ROLLED_BACK, error.result().status());
        assertEquals(0L, count(dataSource));
    }

    /**
     * INDEPENDENT 已确认首片后收到非 VM {@link Error} 时，必须保留已提交分片和原始失败，
     * 同时关闭输入订阅，不能把可恢复结果上下文直接丢失给调用方。
     */
    @Test
    void keepsCommittedIndependentChunksWhenUpstreamThrowsOrdinaryError() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_independent_upstream_ordinary_error");
        createTable(dataSource);
        AssertionError expected = new AssertionError("upstream failed after committed chunk");
        AtomicInteger cancellations = new AtomicInteger();
        Publisher<Object[]> rows = subscriber -> subscriber.onSubscribe(new Subscription() {
            private boolean emitted;

            @Override
            public void request(long requested) {
                if (!emitted) {
                    emitted = true;
                    subscriber.onNext(new Object[]{"committed"});
                    return;
                }
                subscriber.onError(expected);
            }

            @Override
            public void cancel() {
                cancellations.incrementAndGet();
            }
        });

        BatchWriteException error = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(dataSource).writeBatch(request(rows, BatchWriteOptions.independent(1))));

        assertSame(expected, error.getCause());
        assertEquals(BatchWriteResult.Status.PARTIAL, error.result().status());
        assertEquals(2, error.result().chunks().size());
        assertEquals(BatchChunkResult.Status.COMMITTED, error.result().chunks().getFirst().status());
        assertEquals(BatchChunkResult.Status.FAILED, error.result().chunks().get(1).status());
        assertEquals(0, error.result().chunks().get(1).inputCount());
        assertEquals(1L, error.result().affectedRows());
        assertEquals(1L, count(dataSource));
        assertEquals(1, cancellations.get());
    }

    /** INDEPENDENT 在接收首行前终止时也不能把失败批量伪装成空输入的 COMMITTED。 */
    @Test
    void recordsIndependentInputFailureBeforeTheFirstChunk() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_independent_upstream_runtime_error");
        createTable(dataSource);
        IllegalStateException expected = new IllegalStateException("upstream failed before first row");
        AtomicInteger cancellations = new AtomicInteger();
        Publisher<Object[]> rows = subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long requested) {
                subscriber.onError(expected);
            }

            @Override
            public void cancel() {
                cancellations.incrementAndGet();
            }
        });

        BatchWriteException error = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(dataSource).writeBatch(request(rows, BatchWriteOptions.independent(1))));

        assertSame(expected, error.getCause());
        assertEquals(BatchWriteResult.Status.PARTIAL, error.result().status());
        assertEquals(1, error.result().chunks().size());
        assertEquals(BatchChunkResult.Status.FAILED, error.result().chunks().getFirst().status());
        assertEquals(0, error.result().chunks().getFirst().inputCount());
        assertEquals(0L, error.result().inputCount());
        assertEquals(0L, count(dataSource));
        assertEquals(1, cancellations.get());
    }

    /** 生成键逐行路径的影响行数累计超过 {@link Long#MAX_VALUE} 时必须拒绝。 */
    @Test
    void rejectsOverflowingGeneratedKeyJdbcUpdateCounts() throws Exception {
        JdbcDataSource source = dataSource("jdbc_batch_affected_rows_overflow");
        createTable(source);
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into device(name) values (?)", 1, List.of(String.class), SqlBindMarkerStyle.CANONICAL,
                new TrackingPublisher(List.of(new Object[]{"first"}, new Object[]{"second"})),
                BatchWriteOptions.atomic(2), BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.required("id", (offset, key) -> { }),
                BatchWriteCompletion.noop());

        BatchWriteException failure = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(overflowingGeneratedKeyDataSource(source)).writeBatch(request));

        RdbException cause = assertInstanceOf(RdbException.class, failure.getCause());
        assertEquals(RdbErrorKind.UNKNOWN, cause.kind());
        assertEquals("database execution count exceeds supported range", cause.getMessage());
        assertInstanceOf(ArithmeticException.class, cause.getCause());
        assertEquals(BatchWriteResult.Status.ROLLED_BACK, failure.result().status());
        assertEquals(0L, count(source));
    }

    @Test
    void rollsBackAtomicBatchWhenGeneratedKeyConsumerFails() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_generated_key_callback_failure");
        createTable(dataSource);
        AtomicInteger callbacks = new AtomicInteger();
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into device(name) values (?)", 1, List.of(String.class), SqlBindMarkerStyle.CANONICAL,
                new TrackingPublisher(List.of(new Object[]{"a"}, new Object[]{"b"})),
                BatchWriteOptions.atomic(2), BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.required("id", (offset, key) -> {
                    callbacks.incrementAndGet();
                    throw new IllegalStateException("generated key backfill failed");
                }),
                BatchWriteCompletion.noop());

        BatchWriteException error = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(dataSource).writeBatch(request));

        assertEquals(BatchWriteResult.Status.ROLLED_BACK, error.result().status());
        assertEquals(1, callbacks.get());
        assertEquals(0L, count(dataSource));
    }

    /** 用户生成键回调抛出 Error 时也必须先确认回滚，不能把活动事务直接交还连接池。 */
    @Test
    void rollsBackAtomicBatchWhenGeneratedKeyConsumerThrowsError() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_generated_key_error");
        createTable(dataSource);
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into device(name) values (?)", 1, List.of(String.class), SqlBindMarkerStyle.CANONICAL,
                new TrackingPublisher(List.of(new Object[]{"a"}, new Object[]{"b"})),
                BatchWriteOptions.atomic(2), BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.required("id", (offset, key) -> {
                    throw new AssertionError("generated key callback failed");
                }),
                BatchWriteCompletion.noop());

        BatchWriteException error = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(dataSource).writeBatch(request));

        assertInstanceOf(AssertionError.class, error.getCause());
        assertEquals(BatchWriteResult.Status.ROLLED_BACK, error.result().status());
        assertEquals(0L, count(dataSource));
    }

    /** VM 级错误也要先回滚，但不能伪装成带普通批处理结果的业务失败。 */
    @Test
    void rollsBackThenRethrowsVirtualMachineErrorFromAtomicBatch() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_generated_key_vm_error");
        createTable(dataSource);
        OutOfMemoryError expected = new OutOfMemoryError("simulated generated key callback failure");
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into device(name) values (?)", 1, List.of(String.class), SqlBindMarkerStyle.CANONICAL,
                new TrackingPublisher(List.of(new Object[]{"a"}, new Object[]{"b"})),
                BatchWriteOptions.atomic(2), BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.required("id", (offset, key) -> {
                    throw expected;
                }),
                BatchWriteCompletion.noop());

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(dataSource).writeBatch(request));

        assertSame(expected, error);
        assertEquals(0L, count(dataSource));
    }

    /** INDEPENDENT 分片同样需要回滚当前分片并原样传播 VM 级错误。 */
    @Test
    void rollsBackThenRethrowsVirtualMachineErrorFromIndependentBatch() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_independent_generated_key_vm_error");
        createTable(dataSource);
        OutOfMemoryError expected = new OutOfMemoryError("simulated independent generated key callback failure");
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into device(name) values (?)", 1, List.of(String.class), SqlBindMarkerStyle.CANONICAL,
                new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                BatchWriteOptions.independent(1), BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.required("id", (offset, key) -> {
                    throw expected;
                }),
                BatchWriteCompletion.noop());

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(dataSource).writeBatch(request));

        assertSame(expected, error);
        assertEquals(0L, count(dataSource));
    }

    /** 回滚驱动把 VM 错误包在普通异常中时，INDEPENDENT 也不能把它降级为 UNKNOWN 结果。 */
    @Test
    void promotesVirtualMachineErrorNestedInIndependentRollbackFailure() throws Exception {
        JdbcDataSource source = dataSource("independent_nested_rollback_vm_error");
        createTable(source);
        OutOfMemoryError expected = new OutOfMemoryError("rollback cleanup failed");
        PoolProbeDataSource pool = pooledWithFailure(source, "rollback",
                                                      new IllegalStateException("rollback wrapper", expected));

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(duplicateRequest(BatchWriteOptions.independent(2))));

        assertSame(expected, error);
        assertEquals(1, pool.rollbackCalls());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void appliesTheBatchDeadlineToEveryGeneratedKeyRow() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_generated_key_timeout");
        createTable(dataSource);
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into device(name) values (?)", 1, List.of(String.class), SqlBindMarkerStyle.CANONICAL,
                new TrackingPublisher(List.of(new Object[]{"a"}, new Object[]{"b"})),
                BatchWriteOptions.atomic(2).withTimeout(Duration.ofMillis(80)), BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.required("id", (offset, key) -> {
                    try {
                        Thread.sleep(150L);
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("generated key callback was interrupted", error);
                    }
                }),
                BatchWriteCompletion.noop());

        BatchWriteException error = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(dataSource).writeBatch(request));

        assertTrue(error.getCause() instanceof java.util.concurrent.TimeoutException);
        assertEquals(BatchWriteResult.Status.ROLLED_BACK, error.result().status());
        assertEquals(0L, count(dataSource));
    }

    /** INDEPENDENT 的事务时限从分片输入和连接均已可用后开始，不能重复治理上游等待或连接池排队。 */
    @Test
    void startsIndependentChunkTimeoutAfterInputAndConnectionAcquisition() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_independent_timeout_boundary");
        createTable(dataSource);
        BatchWriteOptions options = BatchWriteOptions.independent(1)
                .withTimeout(Duration.ofMillis(100));

        BatchWriteResult result = JdbcBatchWriter.create(delayedConnections(
                dataSource, Duration.ofMillis(200))).writeBatch(request(
                        delayedRows(Duration.ofMillis(200)), options));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1L, count(dataSource));
    }

    /** 普通 batch 的驱动回复晚于绝对截止点时必须拒绝，不能把过期结果交给事务提交。 */
    @Test
    void rejectsRegularBatchReplyThatArrivesAfterDeadline() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_reply_after_deadline");
        createTable(dataSource);
        BatchWriteRequest request = request(new TrackingPublisher(List.<Object[]>of()),
                                            BatchWriteOptions.atomic(1));

        try (Connection delegate = dataSource.getConnection()) {
            delegate.setAutoCommit(false);
            Connection delayed = delayedBatchReplyConnection(delegate, Duration.ofMillis(80));

            assertThrows(java.util.concurrent.TimeoutException.class,
                    () -> new JdbcBatchChunkExecutor().execute(
                            delayed, request, 0, 0L, List.<Object[]>of(new Object[]{"late"}),
                            JdbcBatchSupport.BatchDeadline.start(Duration.ofMillis(20))));
            delegate.rollback();
        }

        assertEquals(0L, count(dataSource));
    }

    /**
     * 普通 JDBC 批量在绑定完成后收到线程中断时，不能再调用驱动的 executeBatch；否则已知取消仍可能写入数据。
     */
    @Test
    void cancelsRegularJdbcBatchBeforeExecutingWhenBindingInterruptsTheCaller() throws Exception {
        JdbcDataSource source = dataSource("jdbc_batch_interrupt_before_execute");
        createTable(source);
        AtomicInteger executeBatchCalls = new AtomicInteger();
        AtomicInteger cancellationCalls = new AtomicInteger();
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into device(name) values (?)", 1, List.of(String.class), SqlBindMarkerStyle.CANONICAL,
                new TrackingPublisher(List.<Object[]>of()), BatchWriteOptions.atomic(1));

        SQLException failure = null;
        try (Connection connection = interruptingBatchConnection(source.getConnection(), executeBatchCalls,
                                                                 cancellationCalls)) {
            try {
                new JdbcBatchChunkExecutor().execute(connection, request, 0, 0L, List.<Object[]>of(new Object[]{"before"}),
                                                      JdbcBatchSupport.BatchDeadline.start(Duration.ZERO));
            } catch (SQLException error) {
                failure = error;
            }
        } finally {
            // 测试夹具故意设置中断位，必须清除以免污染同线程的后续契约测试。
            Thread.interrupted();
        }
        // 先观察目标边界，避免后续输入读取或事务包装掩盖驱动 batch 是否已经被调用。
        assertEquals(0, executeBatchCalls.get());
        assertEquals(1, cancellationCalls.get());
        assertEquals(0L, count(source));

        SQLException cause = assertInstanceOf(SQLException.class, failure);
        assertEquals("HY008", cause.getSQLState());
    }

    @Test
    void acceptsEofImmediatelyAfterTheMaximumResultChunkCount() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_exact_result_chunk_limit");
        createTable(dataSource);
        BatchWriteOptions options = BatchWriteOptions.atomic(2).withMemoryLimits(
                BatchWriteOptions.DEFAULT_MAX_ROWS,
                BatchWriteOptions.DEFAULT_MAX_BUFFERED_BYTES,
                2);

        BatchWriteResult result = JdbcBatchWriter.create(dataSource).writeBatch(request(
                new TrackingPublisher(List.of(new Object[]{"a"}, new Object[]{"b"},
                                                new Object[]{"c"}, new Object[]{"d"})), options));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(2, result.chunks().size());
        assertEquals(4L, count(dataSource));
    }

    @Test
    void rejectsMissingMultipleOrNullGeneratedKeysBeforeAnyBackfill() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_generated_key_validation");
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            try (var missing = statement.executeQuery("select cast(null as bigint) id where 1 = 0")) {
                assertThrows(SQLException.class,
                        () -> JdbcBatchGeneratedKeyReader.readOne(missing, "id", SqlExecutionOptions.safeDefaults()));
            }
            try (var multiple = statement.executeQuery("select cast(1 as bigint) id union all select cast(2 as bigint)")) {
                assertThrows(SQLException.class,
                        () -> JdbcBatchGeneratedKeyReader.readOne(multiple, "id", SqlExecutionOptions.safeDefaults()));
            }
            try (var nullKey = statement.executeQuery("select cast(null as bigint) id")) {
                assertThrows(SQLException.class,
                        () -> JdbcBatchGeneratedKeyReader.readOne(nullKey, "id", SqlExecutionOptions.safeDefaults()));
            }
        }
    }

    /** 驱动返回无效生成键时只报告稳定类别，不能复制无长度上限的调用方列名。 */
    @Test
    void doesNotExposeUnboundedGeneratedKeyColumnInDriverFailure() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_generated_key_message");
        String callerColumn = "secret".repeat(1_000);
        BatchGeneratedKeys generatedKeys = BatchGeneratedKeys.required(callerColumn, (offset, key) -> { });

        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet nullKey = statement.executeQuery("select cast(null as bigint) id")) {
            SQLException failure = assertThrows(SQLException.class, () -> JdbcBatchGeneratedKeyReader.readOne(
                    nullKey, generatedKeys.columnName(), SqlExecutionOptions.safeDefaults()));

            assertEquals("jdbc driver returned an invalid generated key", failure.getMessage());
            assertFalse(failure.getMessage().contains(callerColumn));
        }
    }

    @Test
    void publishesJdbcChunksAndSummaryOnceForTheChunkResultEntryPoint() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_observation");
        createTable(dataSource);
        RecordingBatchObserver observer = new RecordingBatchObserver();
        JdbcBatchWriter writer = JdbcBatchWriter.create(dataSource).withBatchObserver(observer);

        List<BatchChunkResult> chunks = writer.writeBatchChunks(request(
                new TrackingPublisher(List.of(new Object[]{"a"}, new Object[]{"b"})),
                BatchWriteOptions.independent(1)));

        assertEquals(2, chunks.size());
        assertEquals(List.of(BatchExecutionEventType.CHUNK,
                             BatchExecutionEventType.CHUNK,
                             BatchExecutionEventType.SUMMARY),
                     observer.events.stream().map(BatchExecutionObservation::eventType).toList());
        assertEquals(List.of(SqlExecutionBackend.JDBC, SqlExecutionBackend.JDBC, SqlExecutionBackend.JDBC),
                     observer.events.stream().map(BatchExecutionObservation::backend).toList());
        assertEquals(List.of(SqlTransactionSource.INTERNAL, SqlTransactionSource.INTERNAL,
                             SqlTransactionSource.INTERNAL), observer.sources);
    }

    @Test
    void publishesStructuredChunksAndSummaryWhenAtomicWriteFails() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_observation_failure");
        createTable(dataSource);
        RecordingBatchObserver observer = new RecordingBatchObserver();
        BatchWriteRequest request = request(new TrackingPublisher(List.of(
                new Object[]{"a"}, new Object[]{"a"}, new Object[]{"b"})), BatchWriteOptions.atomic(2));

        BatchWriteException error = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(dataSource).withBatchObserver(observer).writeBatch(request));

        assertEquals(error.result().chunks().size() + 1, observer.events.size());
        assertEquals(BatchExecutionEventType.SUMMARY, observer.events.getLast().eventType());
        assertEquals(error.result().status(), observer.events.getLast().summaryStatus());
        assertTrue(observer.events.stream().allMatch(event -> event.backend() == SqlExecutionBackend.JDBC));
    }

    @Test
    void observerFailureDoesNotChangeTheJdbcBatchResult() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_observation_isolated");
        createTable(dataSource);

        BatchWriteResult result = JdbcBatchWriter.create(dataSource).withBatchObserver(
                new BatchExecutionObserver() {
                    @Override
                    public void onExecution(BatchExecutionObservation observation) {
                        throw new IllegalStateException("observer failure");
                    }
                }).writeBatch(request(new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                                      BatchWriteOptions.atomic(1)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1L, count(dataSource));
    }

    /** 已提交批次仍隔离普通观测故障，但包装的 JVM 致命错误必须保留原对象出站。 */
    @Test
    void propagatesVirtualMachineErrorNestedInBatchObserverFailure() throws Exception {
        JdbcDataSource dataSource = dataSource("jdbc_batch_observation_nested_fatal");
        createTable(dataSource);
        OutOfMemoryError fatal = new OutOfMemoryError("batch observer fatal");
        AtomicInteger callbacks = new AtomicInteger();
        JdbcBatchWriter writer = JdbcBatchWriter.create(dataSource).withBatchObserver(ignored -> {
            if (callbacks.getAndIncrement() == 0) {
                throw new IllegalStateException("observer wrapper", fatal);
            }
        });

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, () -> writer.writeBatch(
                request(new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                        BatchWriteOptions.atomic(1))));

        assertSame(fatal, observed);
        assertEquals(1L, count(dataSource));
    }

    @Test
    void commitsAtomicChunksAndRequestsPublisherOneRowAtATime() throws Exception {
        JdbcDataSource dataSource = dataSource("atomic_commit");
        createTable(dataSource);
        TrackingPublisher rows = new TrackingPublisher(List.of(new Object[]{"a"}, new Object[]{"b"}, new Object[]{"c"}));

        BatchWriteResult result = JdbcBatchWriter.create(dataSource).writeBatch(request(rows, BatchWriteOptions.atomic(2)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(3L, result.affectedRows());
        assertEquals(2, result.chunks().size());
        assertEquals(1L, rows.largestRequest());
        assertEquals(3L, count(dataSource));
    }

    @Test
    void closesOwnedPoolLeaseAfterConfirmedAtomicCommit() throws Exception {
        JdbcDataSource source = dataSource("atomic_confirmed_close");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, null);

        BatchWriteResult result = JdbcBatchWriter.create(pool).writeBatch(request(
                new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.atomic(1)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(0, pool.abortCalls());
        assertEquals(1, pool.closeCalls());
    }

    /** 连接归还失败不能把数据库已经确认的 ATOMIC 提交改写成 UNKNOWN。 */
    @Test
    void preservesCommittedAtomicResultWhenLeaseCloseFails() throws Exception {
        JdbcDataSource source = dataSource("atomic_confirmed_close_failure");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "close");
        CleanupProbe observer = new CleanupProbe();

        BatchWriteResult result = JdbcBatchWriter.create(pool).withBatchObserver(observer).writeBatch(request(
                new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.atomic(1)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1L, result.affectedRows());
        assertEquals(1L, count(source));
        assertEquals(1, pool.abortCalls());
        assertConfirmedCloseObservation(observer.singleEvent());
    }

    /** 连接归还失败不能把数据库已经确认的 INDEPENDENT 分片改写成 FAILED。 */
    @Test
    void preservesCommittedIndependentResultWhenLeaseCloseFails() throws Exception {
        JdbcDataSource source = dataSource("independent_confirmed_close_failure");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "close");
        CleanupProbe observer = new CleanupProbe();

        BatchWriteResult result = JdbcBatchWriter.create(pool).withBatchObserver(observer).writeBatch(request(
                new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.independent(1)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(BatchChunkResult.Status.COMMITTED, result.chunks().getFirst().status());
        assertEquals(1L, result.affectedRows());
        assertEquals(1L, count(source));
        assertEquals(1, pool.abortCalls());
        assertConfirmedCloseObservation(observer.singleEvent());
    }

    /** 驱动以普通运行时异常报告连接归还失败时也不能改写已确认提交。 */
    @Test
    void preservesCommittedAtomicResultWhenLeaseCloseThrowsRuntimeException() throws Exception {
        JdbcDataSource source = dataSource("atomic_confirmed_runtime_close_failure");
        createTable(source);
        PoolProbeDataSource pool = pooledWithFailure(
                source, "close", new IllegalStateException("simulated close failure"));

        BatchWriteResult result = JdbcBatchWriter.create(pool).writeBatch(request(
                new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.atomic(1)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1L, count(source));
        assertEquals(1, pool.abortCalls());
    }

    /** 已确认提交只保护普通清理故障，清理异常图中的 JVM 致命错误仍须保持原对象出站。 */
    @Test
    void propagatesNestedVirtualMachineErrorWhenLeaseCloseFailsAfterCommit() throws Exception {
        JdbcDataSource source = dataSource("atomic_confirmed_nested_fatal_close");
        createTable(source);
        OutOfMemoryError expected = new OutOfMemoryError("close fatal");
        PoolProbeDataSource pool = pooledWithFailure(
                source, "close", new IllegalStateException("driver wrapper", expected));

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class, () -> JdbcBatchWriter.create(pool)
                .writeBatch(request(new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                                    BatchWriteOptions.atomic(1))));

        assertSame(expected, error);
        assertEquals(1L, count(source));
        assertEquals(1, pool.abortCalls());
    }

    /** 清理 observer 的普通故障必须被隔离，不能反向改变已经确认的提交结果。 */
    @Test
    void isolatesOrdinaryCleanupObserverFailureAfterConfirmedCommit() throws Exception {
        JdbcDataSource source = dataSource("atomic_confirmed_cleanup_observer_failure");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "close");
        CleanupProbe observer = new CleanupProbe(new IllegalStateException("observer unavailable"));

        BatchWriteResult result = JdbcBatchWriter.create(pool).withBatchObserver(observer).writeBatch(request(
                new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.atomic(1)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1L, count(source));
        assertConfirmedCloseObservation(observer.singleEvent());
    }

    /** 驱动清理只有普通故障时，observer 的 JVM 致命错误仍须在连接隔离后原样传播。 */
    @Test
    void propagatesCleanupObserverVirtualMachineErrorAfterConfirmedCommit() throws Exception {
        JdbcDataSource source = dataSource("atomic_confirmed_cleanup_observer_fatal");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "close");
        OutOfMemoryError expected = new OutOfMemoryError("observer fatal");
        CleanupProbe observer = new CleanupProbe(expected);

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class, () -> JdbcBatchWriter.create(pool)
                .withBatchObserver(observer)
                .writeBatch(request(new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                                    BatchWriteOptions.atomic(1))));

        assertSame(expected, error);
        assertEquals(1L, count(source));
        assertEquals(1, pool.abortCalls());
        assertConfirmedCloseObservation(observer.singleEvent());
        assertTrue(List.of(error.getSuppressed()).stream().anyMatch(SQLException.class::isInstance));
    }

    /** 驱动和 observer 都出现 VME 时，驱动清理 fatal 保持主异常，observer fatal 只作为无环诊断。 */
    @Test
    void keepsDriverVirtualMachineErrorWhenCleanupObserverAlsoFailsFatally() throws Exception {
        JdbcDataSource source = dataSource("atomic_confirmed_driver_and_observer_fatal");
        createTable(source);
        OutOfMemoryError driverFatal = new OutOfMemoryError("driver fatal");
        OutOfMemoryError observerFatal = new OutOfMemoryError("observer fatal");
        PoolProbeDataSource pool = pooledWithFailure(
                source, "close", new IllegalStateException("driver wrapper", driverFatal));
        CleanupProbe observer = new CleanupProbe(observerFatal);

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class, () -> JdbcBatchWriter.create(pool)
                .withBatchObserver(observer)
                .writeBatch(request(new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                                    BatchWriteOptions.atomic(1))));

        assertSame(driverFatal, error);
        assertTrue(List.of(error.getSuppressed()).contains(observerFatal));
        assertFalse(reaches(observerFatal, driverFatal));
        assertConfirmedCloseObservation(observer.singleEvent());
    }

    /** close 后物理淘汰又抛 VME 时，以淘汰 fatal 为主并且仍发布一次已确认清理事件。 */
    @Test
    void keepsAbortVirtualMachineErrorAfterConfirmedCloseFailure() throws Exception {
        JdbcDataSource source = dataSource("atomic_confirmed_abort_fatal");
        createTable(source);
        SQLException closeFailure = new SQLException("simulated close failure", "08006");
        OutOfMemoryError abortFatal = new OutOfMemoryError("abort fatal");
        PoolProbeDataSource pool = pooledWithFailures(source, "close", closeFailure, abortFatal);
        CleanupProbe observer = new CleanupProbe();

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class, () -> JdbcBatchWriter.create(pool)
                .withBatchObserver(observer)
                .writeBatch(request(new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                                    BatchWriteOptions.atomic(1))));

        assertSame(abortFatal, error);
        assertEquals(1, pool.abortCalls());
        assertTrue(List.of(error.getSuppressed()).contains(closeFailure));
        assertConfirmedCloseObservation(observer.singleEvent());
    }

    @Test
    void closesOwnedPoolLeaseAfterConfirmedAtomicRollback() throws Exception {
        JdbcDataSource source = dataSource("atomic_confirmed_rollback_close");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, null);

        BatchWriteException error = assertThrows(BatchWriteException.class, () -> JdbcBatchWriter.create(pool)
                .writeBatch(request(new TrackingPublisher(List.of(
                        new Object[]{"duplicate"}, new Object[]{"duplicate"})), BatchWriteOptions.atomic(2))));

        assertEquals(BatchWriteResult.Status.ROLLED_BACK, error.result().status());
        assertEquals(0, pool.abortCalls());
        assertEquals(1, pool.closeCalls());
    }

    /** 已确认回滚的业务失败保持主异常，连接归还故障只能作为清理上下文。 */
    @Test
    void preservesRolledBackAtomicResultWhenLeaseCloseFails() throws Exception {
        JdbcDataSource source = dataSource("atomic_confirmed_rollback_close_failure");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "close");
        CleanupProbe observer = new CleanupProbe();

        BatchWriteException error = assertThrows(BatchWriteException.class, () -> JdbcBatchWriter.create(pool)
                .withBatchObserver(observer)
                .writeBatch(request(new TrackingPublisher(List.of(
                        new Object[]{"duplicate"}, new Object[]{"duplicate"})), BatchWriteOptions.atomic(2))));

        assertEquals(BatchWriteResult.Status.ROLLED_BACK, error.result().status());
        assertEquals(0L, count(source));
        assertEquals(1, pool.abortCalls());
        assertTrue(List.of(error.getSuppressed()).stream()
                       .anyMatch(SQLException.class::isInstance));
        assertConfirmedCloseObservation(observer.singleEvent());
    }

    @Test
    void discardsAtomicLeaseWhenAutoCommitRestoreFailsAfterCommit() throws Exception {
        JdbcDataSource source = dataSource("atomic_restore_failure_after_commit");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "restoreAutoCommit");

        BatchWriteResult result = JdbcBatchWriter.create(pool).writeBatch(request(
                new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.atomic(1)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void discardsAtomicLeaseWhenAutoCommitRestoreFailsAfterRollback() throws Exception {
        JdbcDataSource source = dataSource("atomic_restore_failure_after_rollback");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "restoreAutoCommit");

        BatchWriteException error = assertThrows(BatchWriteException.class, () -> JdbcBatchWriter.create(pool)
                .writeBatch(request(new TrackingPublisher(List.of(
                        new Object[]{"duplicate"}, new Object[]{"duplicate"})), BatchWriteOptions.atomic(2))));

        assertEquals(BatchWriteResult.Status.ROLLED_BACK, error.result().status());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void discardsIndependentLeaseWhenAutoCommitRestoreFailsAfterCommit() throws Exception {
        JdbcDataSource source = dataSource("independent_restore_failure_after_commit");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "restoreAutoCommit");

        BatchWriteResult result = JdbcBatchWriter.create(pool).writeBatch(request(
                new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.independent(1)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void discardsIndependentLeaseWhenAutoCommitRestoreFailsAfterRollback() throws Exception {
        JdbcDataSource source = dataSource("independent_restore_failure_after_rollback");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "restoreAutoCommit");

        BatchWriteResult result = JdbcBatchWriter.create(pool).writeBatch(request(
                new TrackingPublisher(List.of(new Object[]{"duplicate"}, new Object[]{"duplicate"})),
                BatchWriteOptions.independent(2)));

        assertEquals(BatchWriteResult.Status.PARTIAL, result.status());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    /** 无法读取初始 auto-commit 状态时，即使回滚成功也不能把状态未知的连接普通归还池。 */
    @Test
    void discardsAtomicLeaseWhenInitialAutoCommitStateCannotBeRead() throws Exception {
        JdbcDataSource source = dataSource("jdbc_atomic_initial_auto_commit_unknown");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "getAutoCommit");

        BatchWriteException error = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(request(
                        new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                        BatchWriteOptions.atomic(1))));

        assertEquals(BatchWriteResult.Status.ROLLED_BACK, error.result().status());
        assertEquals(1, pool.rollbackCalls());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
        assertEquals(0L, count(source));
    }

    /** INDEPENDENT 分片也不能在初始 auto-commit 状态不可读后把连接普通归还池。 */
    @Test
    void discardsIndependentLeaseWhenInitialAutoCommitStateCannotBeRead() throws Exception {
        JdbcDataSource source = dataSource("jdbc_independent_initial_auto_commit_unknown");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "getAutoCommit");

        BatchWriteResult result = JdbcBatchWriter.create(pool).writeBatch(request(
                new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                BatchWriteOptions.independent(1)));

        assertEquals(BatchWriteResult.Status.PARTIAL, result.status());
        assertEquals(BatchChunkResult.Status.FAILED, result.chunks().getFirst().status());
        assertEquals(1, pool.rollbackCalls());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
        assertEquals(0L, count(source));
    }

    /** 恢复 auto-commit 的驱动包装异常若携带 VME，ATOMIC 成功结果不能吞掉该致命错误。 */
    @Test
    void promotesVirtualMachineErrorNestedInAtomicAutoCommitRestoreFailure() throws Exception {
        JdbcDataSource source = dataSource("atomic_nested_restore_vm");
        createTable(source);
        OutOfMemoryError expected = new OutOfMemoryError("restore failed");
        IllegalStateException wrapper = new IllegalStateException("driver wrapper", expected);
        PoolProbeDataSource pool = pooledWithFailure(source, "restoreAutoCommit", wrapper);

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class, () -> JdbcBatchWriter.create(pool)
                .writeBatch(request(new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                                    BatchWriteOptions.atomic(1))));

        assertSame(expected, error);
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    /** INDEPENDENT 分片恢复 auto-commit 时也必须从驱动包装异常中恢复同一 VME。 */
    @Test
    void promotesVirtualMachineErrorNestedInIndependentAutoCommitRestoreFailure() throws Exception {
        JdbcDataSource source = dataSource("independent_nested_restore_vm");
        createTable(source);
        OutOfMemoryError expected = new OutOfMemoryError("restore failed");
        IllegalStateException wrapper = new IllegalStateException("driver wrapper", expected);
        PoolProbeDataSource pool = pooledWithFailure(source, "restoreAutoCommit", wrapper);

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class, () -> JdbcBatchWriter.create(pool)
                .writeBatch(request(new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                                    BatchWriteOptions.independent(1))));

        assertSame(expected, error);
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void rollsBackTheWholeAtomicBatchWhenDriverBatchFails() throws Exception {
        JdbcDataSource dataSource = dataSource("atomic_rollback");
        createTable(dataSource);
        BatchWriteRequest request = request(new TrackingPublisher(List.of(
                new Object[]{"first"}, new Object[]{"duplicate"}, new Object[]{"duplicate"})), BatchWriteOptions.atomic(2));

        BatchWriteException error = assertThrows(BatchWriteException.class,
                                                 () -> JdbcBatchWriter.create(dataSource).writeBatch(request));

        assertEquals(BatchWriteResult.Status.ROLLED_BACK, error.result().status());
        assertEquals(0L, count(dataSource));
    }

    @Test
    void returnsEnlistedWithoutFinishingTheExternalTransaction() throws Exception {
        JdbcDataSource dataSource = dataSource("external_atomic");
        createTable(dataSource);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            JdbcBatchWriter writer = JdbcBatchWriter.create(dataSource)
                    .withTransactionParticipant(() -> java.util.Optional.of(
                            JdbcTransactionContext.external(connection, "primary")));

            BatchWriteResult result = writer.writeBatch(request(new TrackingPublisher(List.of(
                    new Object[]{"a"}, new Object[]{"b"})), BatchWriteOptions.atomic(2)));

            assertEquals(BatchWriteResult.Status.ENLISTED, result.status());
            assertEquals(0L, result.affectedRows());
            assertEquals(0L, count(dataSource));
            connection.commit();
        }
        assertEquals(2L, count(dataSource));
    }

    @Test
    void reportsTheFinalExternalTransactionOutcomeToBatchCompletion() throws Exception {
        JdbcDataSource dataSource = dataSource("external_completion");
        createTable(dataSource);
        AtomicReference<JdbcTransactionCompletion.Listener> listener = new AtomicReference<>();
        AtomicReference<BatchWriteResult> completed = new AtomicReference<>();
        RecordingBatchObserver observer = new RecordingBatchObserver();
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            JdbcBatchWriter writer = JdbcBatchWriter.create(dataSource)
                    .withBatchObserver(observer)
                    .withTransactionParticipant(() -> Optional.of(
                            JdbcTransactionContext.external(connection, "primary", callback -> {
                                listener.set(callback);
                                return true;
                            })));
            BatchWriteRequest request = request(
                    new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                    BatchWriteOptions.atomic(1),
                    result -> {
                        completed.set(result);
                        return BatchWriteCompletion.noop().afterCompletion(result);
                    });

            BatchWriteResult enlisted = writer.writeBatch(request);
            assertEquals(BatchWriteResult.Status.ENLISTED, enlisted.status());
            assertEquals(null, completed.get());

            connection.commit();
            listener.get().afterCompletion(TransactionOutcome.COMMITTED);
        }

        assertEquals(BatchWriteResult.Status.COMMITTED, completed.get().status());
        assertEquals(1L, completed.get().affectedRows());
        assertEquals(List.of(BatchWriteResult.Status.ENLISTED, BatchWriteResult.Status.COMMITTED),
                     observer.events.stream()
                             .filter(event -> event.eventType() == BatchExecutionEventType.SUMMARY)
                             .map(BatchExecutionObservation::summaryStatus)
                             .toList());
        assertTrue(observer.sources.stream().allMatch(source -> source == SqlTransactionSource.EXTERNAL));
    }

    @Test
    void rejectsIndependentBeforeItSubscribesToAnExternalTransactionInput() throws Exception {
        JdbcDataSource dataSource = dataSource("independent_external");
        createTable(dataSource);
        TrackingPublisher rows = new TrackingPublisher(List.<Object[]>of(new Object[]{"a"}));
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            JdbcBatchWriter writer = JdbcBatchWriter.create(dataSource)
                    .withTransactionParticipant(() -> java.util.Optional.of(
                            JdbcTransactionContext.external(connection, "primary")));

            assertThrows(IllegalStateException.class,
                         () -> writer.writeBatch(request(rows, BatchWriteOptions.independent(1))));
        }
        assertFalse(rows.subscribed());
    }

    /** ATOMIC 尚未取得连接时没有事务或 SQL 结果，连接失败不能被伪装成提交结果 UNKNOWN。 */
    @Test
    void reportsFailedBeforeTransactionWhenAtomicConnectionAcquisitionFails() {
        SQLException acquisitionFailure = new SQLException("database connection unavailable", "08001");
        AtomicInteger acquisitionCalls = new AtomicInteger();
        TrackingPublisher rows = new TrackingPublisher(List.<Object[]>of(new Object[]{"a"}));

        BatchWriteException failure = assertThrows(
                BatchWriteException.class,
                () -> JdbcBatchWriter.create(failingDataSource(acquisitionFailure, acquisitionCalls))
                        .writeBatch(request(rows, BatchWriteOptions.atomic(1))));

        assertEquals(1, acquisitionCalls.get());
        assertFalse(rows.subscribed());
        assertSame(acquisitionFailure, failure.getCause());
        assertEquals(BatchWriteResult.Status.ROLLED_BACK, failure.result().status());
        assertEquals(1, failure.result().chunks().size());
        assertEquals(BatchChunkResult.Status.FAILED, failure.result().chunks().getFirst().status());
        assertEquals(0, failure.result().chunks().getFirst().inputCount());
    }

    @Test
    void reportsUnknownWhenTheCommitReplyIsLost() throws Exception {
        JdbcDataSource source = dataSource("unknown_commit");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "commit", true);

        BatchWriteException error = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(
                        request(new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.atomic(1))));

        assertEquals(BatchWriteResult.Status.UNKNOWN, error.result().status());
        assertEquals(1, error.result().chunks().size());
        assertTrue(error.result().chunks().getFirst().status().name().equals("UNKNOWN"));
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
        assertEquals(1, error.getCause().getSuppressed().length);
    }

    /**
     * 空 ATOMIC 批量同样会进入自有事务的提交路径；提交回执丢失时不能因没有已完成分片而被汇总成 COMMITTED。
     */
    @Test
    void reportsUnknownWhenEmptyAtomicBatchCommitReplyIsLost() throws Exception {
        JdbcDataSource source = dataSource("unknown_empty_commit");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "commit", true);

        BatchWriteException error = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(
                        request(new TrackingPublisher(List.<Object[]>of()), BatchWriteOptions.atomic(1))));

        assertEquals(BatchWriteResult.Status.UNKNOWN, error.result().status());
        assertEquals(1, error.result().chunks().size());
        assertEquals(BatchChunkResult.Status.UNKNOWN, error.result().chunks().getFirst().status());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void continuesIndependentChunksAndKeepsEveryKnownResult() throws Exception {
        JdbcDataSource dataSource = dataSource("independent_partial");
        createTable(dataSource);

        BatchWriteResult result = JdbcBatchWriter.create(dataSource).writeBatch(request(
                new TrackingPublisher(List.of(new Object[]{"a"}, new Object[]{"a"}, new Object[]{"b"})),
                BatchWriteOptions.independent(1)));

        assertEquals(BatchWriteResult.Status.PARTIAL, result.status());
        assertEquals(List.of("COMMITTED", "FAILED", "COMMITTED"),
                     result.chunks().stream().map(chunk -> chunk.status().name()).toList());
        assertEquals(2L, result.affectedRows());
        assertEquals(2L, count(dataSource));
    }

    @Test
    void reportsUnknownWhenAtomicRollbackCannotBeConfirmed() throws Exception {
        JdbcDataSource source = dataSource("unknown_rollback");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "rollback");
        BatchWriteRequest request = request(new TrackingPublisher(List.of(
                new Object[]{"duplicate"}, new Object[]{"duplicate"})), BatchWriteOptions.atomic(2));

        BatchWriteException error = assertThrows(BatchWriteException.class,
                                                  () -> JdbcBatchWriter.create(pool)
                                                                       .writeBatch(request));

        assertEquals(BatchWriteResult.Status.UNKNOWN, error.result().status());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void discardsIndependentLeaseWhenCommitReplyIsLost() throws Exception {
        JdbcDataSource source = dataSource("independent_unknown_commit");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "commit");

        BatchWriteResult result = JdbcBatchWriter.create(pool).writeBatch(request(
                new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.independent(1)));

        assertEquals(BatchWriteResult.Status.UNKNOWN, result.status());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void discardsIndependentLeaseWhenRollbackCannotBeConfirmed() throws Exception {
        JdbcDataSource source = dataSource("independent_unknown_rollback");
        createTable(source);
        PoolProbeDataSource pool = pooled(source, "rollback");

        BatchWriteResult result = JdbcBatchWriter.create(pool).writeBatch(request(
                new TrackingPublisher(List.of(new Object[]{"duplicate"}, new Object[]{"duplicate"})),
                BatchWriteOptions.independent(2)));

        assertEquals(BatchWriteResult.Status.UNKNOWN, result.status());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void rechecksExternalTransactionAfterIndependentPreflight() throws Exception {
        JdbcDataSource dataSource = dataSource("independent_transaction_race");
        createTable(dataSource);
        AtomicInteger checks = new AtomicInteger();
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            JdbcBatchWriter writer = JdbcBatchWriter.create(dataSource).withTransactionParticipant(() ->
                    checks.getAndIncrement() == 0
                            ? Optional.empty()
                            : Optional.of(JdbcTransactionContext.external(connection, "primary")));

            assertThrows(IllegalStateException.class, () -> writer.writeBatch(request(
                    new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.independent(1))));
            connection.rollback();
        }
        assertEquals(0L, count(dataSource));
    }

    @Test
    void marksAtomicResultUnknownAndDiscardsLeaseWhenRollbackThrowsRuntimeException() throws Exception {
        JdbcDataSource source = dataSource("atomic_runtime_rollback");
        createTable(source);
        PoolProbeDataSource pool = pooledWithFailure(source, "rollback", new IllegalStateException("rollback failed"));

        BatchWriteException error = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(duplicateRequest(BatchWriteOptions.atomic(2))));

        assertEquals(BatchWriteResult.Status.UNKNOWN, error.result().status());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void marksIndependentResultUnknownAndDiscardsLeaseWhenRollbackThrowsRuntimeException() throws Exception {
        JdbcDataSource source = dataSource("independent_runtime_rollback");
        createTable(source);
        PoolProbeDataSource pool = pooledWithFailure(source, "rollback", new IllegalStateException("rollback failed"));

        BatchWriteResult result = JdbcBatchWriter.create(pool).writeBatch(
                duplicateRequest(BatchWriteOptions.independent(2)));

        assertEquals(BatchWriteResult.Status.UNKNOWN, result.status());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void keepsAtomicVirtualMachineErrorWhenRollbackThrowsRuntimeException() throws Exception {
        JdbcDataSource source = dataSource("atomic_vm_runtime_rollback");
        createTable(source);
        IllegalStateException rollbackFailure = new IllegalStateException("rollback failed");
        PoolProbeDataSource pool = pooledWithFailure(source, "rollback", rollbackFailure);
        OutOfMemoryError expected = new OutOfMemoryError("simulated callback failure");

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(vmErrorRequest(BatchWriteOptions.atomic(1), expected)));

        assertSame(expected, error);
        assertEquals(1, error.getSuppressed().length);
        assertSame(rollbackFailure, error.getSuppressed()[0]);
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void keepsIndependentVirtualMachineErrorWhenRollbackThrowsRuntimeException() throws Exception {
        JdbcDataSource source = dataSource("independent_vm_runtime_rollback");
        createTable(source);
        IllegalStateException rollbackFailure = new IllegalStateException("rollback failed");
        PoolProbeDataSource pool = pooledWithFailure(source, "rollback", rollbackFailure);
        OutOfMemoryError expected = new OutOfMemoryError("simulated callback failure");

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(vmErrorRequest(BatchWriteOptions.independent(1), expected)));

        assertSame(expected, error);
        assertEquals(1, error.getSuppressed().length);
        assertSame(rollbackFailure, error.getSuppressed()[0]);
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void marksAtomicResultUnknownAndDiscardsLeaseWhenCommitThrowsRuntimeException() throws Exception {
        JdbcDataSource source = dataSource("atomic_runtime_commit");
        createTable(source);
        PoolProbeDataSource pool = pooledWithFailure(source, "commit", new IllegalStateException("commit failed"));

        BatchWriteException error = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(request(
                        new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.atomic(1))));

        assertEquals(BatchWriteResult.Status.UNKNOWN, error.result().status());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    /** 提交结果未知且隔离连接失败时，abort 的 VM 错误不能被普通批量异常隐藏。 */
    @Test
    void promotesAbortVirtualMachineErrorOverAtomicCommitFailure() throws Exception {
        JdbcDataSource source = dataSource("atomic_commit_abort_vm");
        createTable(source);
        IllegalStateException commitFailure = new IllegalStateException("commit failed");
        OutOfMemoryError abortFailure = new OutOfMemoryError("abort failed");
        PoolProbeDataSource pool = pooledWithFailures(source, "commit", commitFailure, abortFailure);

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(request(
                        new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.atomic(1))));

        assertSame(abortFailure, error);
        assertTrue(List.of(error.getSuppressed()).contains(commitFailure));
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
        assertEquals(0, pool.rollbackCalls());
    }

    @Test
    void marksIndependentResultUnknownAndDiscardsLeaseWhenCommitThrowsRuntimeException() throws Exception {
        JdbcDataSource source = dataSource("independent_runtime_commit");
        createTable(source);
        PoolProbeDataSource pool = pooledWithFailure(source, "commit", new IllegalStateException("commit failed"));

        BatchWriteResult result = JdbcBatchWriter.create(pool).writeBatch(request(
                new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.independent(1)));

        assertEquals(BatchWriteResult.Status.UNKNOWN, result.status());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    @Test
    void keepsAtomicVirtualMachineErrorAndDiscardsLeaseWhenCommitThrowsIt() throws Exception {
        JdbcDataSource source = dataSource("atomic_vm_commit");
        createTable(source);
        OutOfMemoryError expected = new OutOfMemoryError("simulated commit failure");
        PoolProbeDataSource pool = pooledWithFailure(source, "commit", expected);

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(request(
                        new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.atomic(1))));

        assertSame(expected, error);
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
        assertEquals(0, pool.rollbackCalls());
    }

    @Test
    void keepsIndependentVirtualMachineErrorAndDiscardsLeaseWhenCommitThrowsIt() throws Exception {
        JdbcDataSource source = dataSource("independent_vm_commit");
        createTable(source);
        OutOfMemoryError expected = new OutOfMemoryError("simulated commit failure");
        PoolProbeDataSource pool = pooledWithFailure(source, "commit", expected);

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(request(
                        new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.independent(1))));

        assertSame(expected, error);
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
        assertEquals(0, pool.rollbackCalls());
    }

    /** INDEPENDENT 提交失败包装的 VME 必须在连接隔离完成后恢复原对象。 */
    @Test
    void promotesVirtualMachineErrorNestedInIndependentCommitFailure() throws Exception {
        JdbcDataSource source = dataSource("independent_nested_vm_commit");
        createTable(source);
        OutOfMemoryError expected = new OutOfMemoryError("simulated commit failure");
        PoolProbeDataSource pool = pooledWithFailure(
                source, "commit", new IllegalStateException("driver wrapper", expected));

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(request(
                        new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                        BatchWriteOptions.independent(1))));

        assertSame(expected, error);
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
        assertEquals(0, pool.rollbackCalls());
    }

    /** INDEPENDENT 初始化失败包装的 VME 必须先确认回滚，再恢复同一致命错误。 */
    @Test
    void promotesVirtualMachineErrorNestedInIndependentSetupFailureAfterRollback() throws Exception {
        JdbcDataSource source = dataSource("independent_nested_vm_setup");
        createTable(source);
        OutOfMemoryError expected = new OutOfMemoryError("simulated setup failure");
        PoolProbeDataSource pool = pooledWithFailure(
                source, "disableAutoCommit", new IllegalStateException("driver wrapper", expected));

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(request(
                        new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                        BatchWriteOptions.independent(1))));

        assertSame(expected, error);
        assertEquals(1, pool.rollbackCalls());
        assertEquals(0, pool.abortCalls());
        assertEquals(1, pool.closeCalls());
    }

    /** 事务初始化的驱动运行时异常也必须进入已有回滚和会话状态恢复路径。 */
    @Test
    void rollsBackAndRestoresAutoCommitWhenIndependentSetupThrowsRuntimeException() throws Exception {
        JdbcDataSource source = dataSource("independent_runtime_setup");
        createTable(source);
        PoolProbeDataSource pool = pooledWithFailure(source, "disableAutoCommit",
                new IllegalStateException("set auto commit failed"));

        BatchWriteResult result = JdbcBatchWriter.create(pool).writeBatch(request(
                new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.independent(1)));

        assertEquals(BatchWriteResult.Status.PARTIAL, result.status());
        assertEquals(BatchChunkResult.Status.FAILED, result.chunks().getFirst().status());
        assertEquals(1, pool.rollbackCalls());
        assertEquals(0, pool.abortCalls());
        assertEquals(1, pool.closeCalls());
    }

    /** 驱动即使在抛错前已经切换状态，ATOMIC 回滚确认后仍必须恢复 auto-commit。 */
    @Test
    void restoresAutoCommitAfterAtomicPartialDisableFailure() throws Exception {
        JdbcDataSource source = dataSource("atomic_partial_disable");
        createTable(source);
        PoolProbeDataSource pool = pooledWithFailure(source, "partialDisableAutoCommit",
                new IllegalStateException("set auto commit failed after state changed"));

        BatchWriteException error = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(request(
                        new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.atomic(1))));

        assertEquals(BatchWriteResult.Status.ROLLED_BACK, error.result().status());
        assertEquals(1, pool.rollbackCalls());
        assertEquals(1, pool.restoreAutoCommitCalls());
        assertEquals(0, pool.abortCalls());
        assertEquals(1, pool.closeCalls());
    }

    /** 初始化阶段的 VM 错误不降级，同时仍确认回滚并恢复本次改过的连接状态。 */
    @Test
    void keepsVirtualMachineErrorWhenIndependentSetupThrowsIt() throws Exception {
        JdbcDataSource source = dataSource("independent_vm_setup");
        createTable(source);
        OutOfMemoryError expected = new OutOfMemoryError("set auto commit failed");
        PoolProbeDataSource pool = pooledWithFailure(source, "disableAutoCommit", expected);

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(request(
                        new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})), BatchWriteOptions.independent(1))));

        assertSame(expected, error);
        assertEquals(1, pool.rollbackCalls());
        assertEquals(0, pool.abortCalls());
        assertEquals(1, pool.closeCalls());
    }

    /** 已有业务 VM 错误不能被 finally 的会话恢复 VM 错误覆盖。 */
    @Test
    void keepsAtomicOperationVirtualMachineErrorWhenRestoreAutoCommitThrowsIt() throws Exception {
        JdbcDataSource source = dataSource("atomic_operation_vm_restore_vm");
        createTable(source);
        OutOfMemoryError operationFailure = new OutOfMemoryError("operation failed");
        OutOfMemoryError restoreFailure = new OutOfMemoryError("restore failed");
        PoolProbeDataSource pool = pooledWithFailure(source, "restoreAutoCommit", restoreFailure);

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(vmErrorRequest(BatchWriteOptions.atomic(1), operationFailure)));

        assertSame(operationFailure, error);
        assertTrue(List.of(error.getSuppressed()).contains(restoreFailure));
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    /** INDEPENDENT 分片同样保留原始 VM 错误并把恢复故障作为辅助上下文。 */
    @Test
    void keepsIndependentOperationVirtualMachineErrorWhenRestoreAutoCommitThrowsIt() throws Exception {
        JdbcDataSource source = dataSource("independent_operation_vm_restore_vm");
        createTable(source);
        OutOfMemoryError operationFailure = new OutOfMemoryError("operation failed");
        OutOfMemoryError restoreFailure = new OutOfMemoryError("restore failed");
        PoolProbeDataSource pool = pooledWithFailure(source, "restoreAutoCommit", restoreFailure);

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(vmErrorRequest(
                        BatchWriteOptions.independent(1), operationFailure)));

        assertSame(operationFailure, error);
        assertTrue(List.of(error.getSuppressed()).contains(restoreFailure));
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    /** 仅清理阶段出现 VM 错误时，它应优先于普通批量失败并保留结果上下文。 */
    @Test
    void promotesRestoreVirtualMachineErrorOverAtomicBatchFailure() throws Exception {
        JdbcDataSource source = dataSource("atomic_failure_restore_vm");
        createTable(source);
        OutOfMemoryError restoreFailure = new OutOfMemoryError("restore failed");
        PoolProbeDataSource pool = pooledWithFailure(source, "restoreAutoCommit", restoreFailure);

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(pool).writeBatch(duplicateRequest(BatchWriteOptions.atomic(2))));

        assertSame(restoreFailure, error);
        BatchWriteException operationFailure = assertInstanceOf(BatchWriteException.class, error.getSuppressed()[0]);
        assertEquals(BatchWriteResult.Status.ROLLED_BACK, operationFailure.result().status());
        assertEquals(1, pool.abortCalls());
        assertEquals(0, pool.closeCalls());
    }

    /** TWR 将清理 VME 直接压到主异常时，提升 VME 不能反向构造 Throwable 环。 */
    @Test
    void promotesDirectlySuppressedVirtualMachineErrorWithoutCreatingThrowableCycle() {
        BatchWriteException primary = new BatchWriteException("batch failed",
                new IllegalStateException("operation failed"),
                BatchWriteResult.empty(BatchWriteOptions.Mode.ATOMIC));
        OutOfMemoryError fatal = new OutOfMemoryError("close failed");
        primary.addSuppressed(fatal);

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchSupport.rethrowSuppressedVirtualMachineError(primary));

        assertSame(fatal, error);
        assertEquals(1, primary.getSuppressed().length);
        assertSame(fatal, primary.getSuppressed()[0]);
        assertEquals(0, fatal.getSuppressed().length);
    }

    /**
     * JDBC 输入订阅在关闭时抛出的异常可能把 VME 藏在 cause 中；最外层事务清理完成后仍必须保留该 VME
     * 的原始身份，不能用 BatchWriteException 包装。
     */
    @Test
    void promotesVirtualMachineErrorNestedInBatchPublisherCancellationFailure() throws Exception {
        JdbcDataSource source = dataSource("atomic_nested_cancel_vm_error");
        createTable(source);
        OutOfMemoryError expected = new OutOfMemoryError("cancel cleanup failed");
        AtomicReference<IllegalStateException> cancellationFailure = new AtomicReference<>();
        Publisher<Object[]> rows = subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long ignored) {
                subscriber.onError(new IllegalStateException("input failed"));
            }

            @Override
            public void cancel() {
                IllegalStateException failure = new IllegalStateException("cancel failed", expected);
                cancellationFailure.set(failure);
                throw failure;
            }
        });

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class,
                () -> JdbcBatchWriter.create(source).writeBatch(request(rows, BatchWriteOptions.atomic(1))));

        assertSame(expected, error);
        assertSame(expected, cancellationFailure.get().getCause());
        assertEquals(0, expected.getSuppressed().length);
        assertEquals(0L, count(source));
    }

    private static BatchWriteRequest request(Publisher<Object[]> rows, BatchWriteOptions options) {
        return new BatchWriteRequest("insert into device(name) values (?)", 1, List.of(String.class),
                                     SqlBindMarkerStyle.CANONICAL, rows, options);
    }

    private static BatchWriteRequest duplicateRequest(BatchWriteOptions options) {
        return request(new TrackingPublisher(List.of(new Object[]{"duplicate"}, new Object[]{"duplicate"})), options);
    }

    private static BatchWriteRequest vmErrorRequest(BatchWriteOptions options, OutOfMemoryError error) {
        return new BatchWriteRequest("insert into device(name) values (?)", 1, List.of(String.class),
                SqlBindMarkerStyle.CANONICAL, new TrackingPublisher(List.<Object[]>of(new Object[]{"a"})),
                options, BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.required("id", (offset, key) -> { throw error; }),
                BatchWriteCompletion.noop());
    }

    private static BatchWriteRequest request(Publisher<Object[]> rows,
                                             BatchWriteOptions options,
                                             BatchWriteCompletion completion) {
        return new BatchWriteRequest("insert into device(name) values (?)", 1, List.of(String.class),
                                     SqlBindMarkerStyle.CANONICAL, rows, options,
                                     com.flying.orm.rdb.batch.BatchRowCountPolicy.ANY, completion);
    }

    private static JdbcDataSource dataSource(String name) {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        return dataSource;
    }

    private static DataSource failingDataSource(SQLException failure, AtomicInteger acquisitionCalls) {
        return new DataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                acquisitionCalls.incrementAndGet();
                throw failure;
            }

            @Override
            public Connection getConnection(String username, String password) throws SQLException {
                acquisitionCalls.incrementAndGet();
                throw failure;
            }

            @Override public PrintWriter getLogWriter() { return null; }
            @Override public void setLogWriter(PrintWriter out) { }
            @Override public void setLoginTimeout(int seconds) { }
            @Override public int getLoginTimeout() { return 0; }
            @Override public Logger getParentLogger() { return Logger.getGlobal(); }
            @Override public <T> T unwrap(Class<T> type) throws SQLException {
                throw new SQLException("failing data source cannot be unwrapped");
            }
            @Override public boolean isWrapperFor(Class<?> type) { return false; }
        };
    }

    private static Publisher<Object[]> delayedRows(Duration delay) {
        return subscriber -> subscriber.onSubscribe(new Subscription() {
            private boolean emitted;

            @Override
            public void request(long requested) {
                if (emitted || requested <= 0L) {
                    return;
                }
                emitted = true;
                try {
                    Thread.sleep(delay.toMillis());
                    subscriber.onNext(new Object[]{"delayed"});
                    subscriber.onComplete();
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    subscriber.onError(error);
                }
            }

            @Override
            public void cancel() {
                // 同步测试 Publisher 没有额外资源需要释放。
            }
        });
    }

    private static DataSource delayedConnections(DataSource delegate, Duration delay) {
        return new DataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                delayConnectionAcquisition(delay);
                return delegate.getConnection();
            }

            @Override
            public Connection getConnection(String username, String password) throws SQLException {
                delayConnectionAcquisition(delay);
                return delegate.getConnection(username, password);
            }

            @Override public PrintWriter getLogWriter() throws SQLException { return delegate.getLogWriter(); }
            @Override public void setLogWriter(PrintWriter out) throws SQLException { delegate.setLogWriter(out); }
            @Override public void setLoginTimeout(int seconds) throws SQLException { delegate.setLoginTimeout(seconds); }
            @Override public int getLoginTimeout() throws SQLException { return delegate.getLoginTimeout(); }
            @Override public Logger getParentLogger() { return Logger.getGlobal(); }
            @Override public <T> T unwrap(Class<T> type) throws SQLException { return delegate.unwrap(type); }
            @Override public boolean isWrapperFor(Class<?> type) throws SQLException {
                return delegate.isWrapperFor(type);
            }
        };
    }

    private static void delayConnectionAcquisition(Duration delay) throws SQLException {
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new SQLException("delayed connection acquisition was interrupted", "HY008", error);
        }
    }

    private static void createTable(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table device (id bigint generated by default as identity primary key, "
                    + "name varchar(128) not null unique)");
        }
    }

    private static void createNullableTable(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table nullable_device (id bigint generated by default as identity primary key, "
                    + "note varchar(128))");
        }
    }

    private static long count(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             java.sql.ResultSet result = statement.executeQuery("select count(*) from device")) {
            result.next();
            return result.getLong(1);
        }
    }

    private static String onlyName(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             java.sql.ResultSet result = statement.executeQuery("select name from device")) {
            result.next();
            return result.getString(1);
        }
    }

    private static long nullableCount(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             java.sql.ResultSet result = statement.executeQuery("select count(*) from nullable_device")) {
            result.next();
            return result.getLong(1);
        }
    }

    private static PoolProbeDataSource pooled(DataSource delegate, String failingMethod) {
        return pooled(delegate, failingMethod, false);
    }

    private static DataSource overflowingGeneratedKeyDataSource(DataSource delegate) {
        return new DataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                return overflowingGeneratedKeyConnection(delegate.getConnection());
            }

            @Override
            public Connection getConnection(String username, String password) throws SQLException {
                return overflowingGeneratedKeyConnection(delegate.getConnection(username, password));
            }

            @Override public PrintWriter getLogWriter() throws SQLException { return delegate.getLogWriter(); }
            @Override public void setLogWriter(PrintWriter out) throws SQLException { delegate.setLogWriter(out); }
            @Override public void setLoginTimeout(int seconds) throws SQLException { delegate.setLoginTimeout(seconds); }
            @Override public int getLoginTimeout() throws SQLException { return delegate.getLoginTimeout(); }
            @Override public Logger getParentLogger() { return Logger.getGlobal(); }
            @Override public <T> T unwrap(Class<T> type) throws SQLException { return delegate.unwrap(type); }
            @Override public boolean isWrapperFor(Class<?> type) throws SQLException { return delegate.isWrapperFor(type); }
        };
    }

    private static Connection interruptingBatchConnection(Connection delegate,
                                                           AtomicInteger executeBatchCalls,
                                                           AtomicInteger cancellationCalls) {
        return (Connection) Proxy.newProxyInstance(JdbcBatchWriterTest.class.getClassLoader(), new Class[]{Connection.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(delegate, arguments);
                        if (method.getName().equals("prepareStatement") && result instanceof PreparedStatement statement
                                && (arguments == null || arguments.length == 1)) {
                            return interruptingBatchStatement(statement, executeBatchCalls, cancellationCalls);
                        }
                        return result;
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    }
                });
    }

    private static Connection delayedBatchReplyConnection(Connection delegate, Duration delay) {
        return (Connection) Proxy.newProxyInstance(
                JdbcBatchWriterTest.class.getClassLoader(), new Class[]{Connection.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(delegate, arguments);
                        if (method.getName().equals("prepareStatement")
                                && result instanceof PreparedStatement statement
                                && (arguments == null || arguments.length == 1)) {
                            return delayedBatchReplyStatement(statement, delay);
                        }
                        return result;
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    }
                });
    }

    private static Connection interruptingOwnerQueryConnection(Connection delegate,
                                                               AtomicInteger nextCalls,
                                                               AtomicInteger valueReads,
                                                               AtomicInteger cancellationCalls) {
        return (Connection) Proxy.newProxyInstance(
                JdbcBatchWriterTest.class.getClassLoader(), new Class[]{Connection.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(delegate, arguments);
                        if (method.getName().equals("prepareStatement")
                                && result instanceof PreparedStatement statement
                                && arguments != null && arguments.length == 1) {
                            return interruptingOwnerStatement(
                                    statement, nextCalls, valueReads, cancellationCalls);
                        }
                        return result;
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    }
                });
    }

    private static Array jdbcArray(Object value, AtomicBoolean freed) {
        return (Array) Proxy.newProxyInstance(
                JdbcBatchWriterTest.class.getClassLoader(), new Class[]{Array.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getArray" -> value;
                    case "free" -> {
                        freed.set(true);
                        yield null;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Connection protectedOwnerConnection(Array array) {
        ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(
                JdbcBatchWriterTest.class.getClassLoader(), new Class[]{ResultSetMetaData.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getColumnCount" -> 1;
                    case "getColumnLabel", "getColumnName" -> "id";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        AtomicInteger rows = new AtomicInteger();
        ResultSet resultSet = (ResultSet) Proxy.newProxyInstance(
                JdbcBatchWriterTest.class.getClassLoader(), new Class[]{ResultSet.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getMetaData" -> metadata;
                    case "next" -> rows.getAndIncrement() == 0;
                    case "getObject" -> array;
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                JdbcBatchWriterTest.class.getClassLoader(), new Class[]{PreparedStatement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "executeQuery" -> resultSet;
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (Connection) Proxy.newProxyInstance(
                JdbcBatchWriterTest.class.getClassLoader(), new Class[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "prepareStatement" -> statement;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static PreparedStatement interruptingOwnerStatement(PreparedStatement delegate,
                                                                 AtomicInteger nextCalls,
                                                                 AtomicInteger valueReads,
                                                                 AtomicInteger cancellationCalls) {
        return (PreparedStatement) Proxy.newProxyInstance(
                JdbcBatchWriterTest.class.getClassLoader(), new Class[]{PreparedStatement.class},
                (proxy, method, arguments) -> {
                    try {
                        if (method.getName().equals("cancel")) {
                            cancellationCalls.incrementAndGet();
                        }
                        Object result = method.invoke(delegate, arguments);
                        if (method.getName().equals("executeQuery") && result instanceof ResultSet resultSet) {
                            return interruptingOwnerResultSet(resultSet, nextCalls, valueReads);
                        }
                        return result;
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    }
                });
    }

    private static ResultSet interruptingOwnerResultSet(ResultSet delegate,
                                                        AtomicInteger nextCalls,
                                                        AtomicInteger valueReads) {
        return (ResultSet) Proxy.newProxyInstance(
                JdbcBatchWriterTest.class.getClassLoader(), new Class[]{ResultSet.class},
                (proxy, method, arguments) -> {
                    try {
                        if (method.getName().equals("getObject")) {
                            valueReads.incrementAndGet();
                        }
                        Object result = method.invoke(delegate, arguments);
                        if (method.getName().equals("next")) {
                            nextCalls.incrementAndGet();
                            if (Boolean.TRUE.equals(result)) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        return result;
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    }
                });
    }

    private static Connection recordingConnection(Connection delegate, List<String> preparedSql) {
        return (Connection) Proxy.newProxyInstance(
                JdbcBatchWriterTest.class.getClassLoader(), new Class[]{Connection.class},
                (proxy, method, arguments) -> {
                    try {
                        if (method.getName().equals("prepareStatement") && arguments != null
                                && arguments.length > 0 && arguments[0] instanceof String sql) {
                            preparedSql.add(sql);
                        }
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    }
                });
    }

    private static PreparedStatement interruptingBatchStatement(PreparedStatement delegate,
                                                                 AtomicInteger executeBatchCalls,
                                                                 AtomicInteger cancellationCalls) {
        return (PreparedStatement) Proxy.newProxyInstance(
                JdbcBatchWriterTest.class.getClassLoader(), new Class[]{PreparedStatement.class},
                (proxy, method, arguments) -> {
                    try {
                        if (method.getName().equals("executeBatch")) {
                            executeBatchCalls.incrementAndGet();
                        } else if (method.getName().equals("cancel")) {
                            cancellationCalls.incrementAndGet();
                        }
                        Object result = method.invoke(delegate, arguments);
                        if (method.getName().equals("addBatch")) {
                            Thread.currentThread().interrupt();
                        }
                        return result;
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    }
                });
    }

    private static PreparedStatement delayedBatchReplyStatement(PreparedStatement delegate, Duration delay) {
        return (PreparedStatement) Proxy.newProxyInstance(
                JdbcBatchWriterTest.class.getClassLoader(), new Class[]{PreparedStatement.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(delegate, arguments);
                        if (method.getName().equals("executeBatch")) {
                            Thread.sleep(delay.toMillis());
                        }
                        return result;
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    }
                });
    }

    private static Connection overflowingGeneratedKeyConnection(Connection delegate) {
        AtomicInteger updateCalls = new AtomicInteger();
        return (Connection) Proxy.newProxyInstance(JdbcBatchWriterTest.class.getClassLoader(), new Class[]{Connection.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(delegate, arguments);
                        if (method.getName().equals("prepareStatement")
                                && result instanceof PreparedStatement statement
                                && arguments != null
                                && arguments.length == 2
                                && arguments[1] instanceof String[]) {
                            return overflowingGeneratedKeyStatement(statement, updateCalls);
                        }
                        return result;
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    }
                });
    }

    private static PreparedStatement overflowingGeneratedKeyStatement(PreparedStatement delegate,
                                                                        AtomicInteger updateCalls) {
        return (PreparedStatement) Proxy.newProxyInstance(
                JdbcBatchWriterTest.class.getClassLoader(), new Class[]{PreparedStatement.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(delegate, arguments);
                        if (method.getName().equals("executeLargeUpdate")) {
                            return updateCalls.getAndIncrement() == 0 ? Long.MAX_VALUE : 1L;
                        }
                        return result;
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    }
                });
    }

    private static PoolProbeDataSource pooled(DataSource delegate, String failingMethod, boolean failAbort) {
        return new PoolProbeDataSource(delegate, failingMethod, failAbort);
    }

    private static PoolProbeDataSource pooledWithFailure(DataSource delegate, String failingMethod, Throwable failure) {
        return new PoolProbeDataSource(delegate, failingMethod, false, failure);
    }

    private static PoolProbeDataSource pooledWithFailures(DataSource delegate,
                                                          String failingMethod,
                                                          Throwable failure,
                                                          Throwable abortFailure) {
        return new PoolProbeDataSource(delegate, failingMethod, false, failure, abortFailure);
    }

    private static void assertConfirmedCloseObservation(ResourceCleanupObservation observation) {
        assertEquals(SqlExecutionOperation.CHUNKED_BATCH_WRITE, observation.operation());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_CLOSE, observation.phase());
        assertTrue(observation.outcomeConfirmed());
    }

    private static boolean reaches(Throwable root, Throwable target) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == target) {
                return true;
            }
            if (current.getCause() != null) {
                pending.addLast(current.getCause());
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return false;
    }

    private static final class PoolProbeDataSource implements DataSource {

        private final DataSource delegate;
        private final String failingMethod;
        private final boolean failAbort;
        private final Throwable injectedFailure;
        private final Throwable abortFailure;
        private final AtomicInteger abortCalls = new AtomicInteger();
        private final AtomicInteger closeCalls = new AtomicInteger();
        private final AtomicInteger rollbackCalls = new AtomicInteger();
        private final AtomicInteger restoreAutoCommitCalls = new AtomicInteger();

        private PoolProbeDataSource(DataSource delegate, String failingMethod, boolean failAbort) {
            this(delegate, failingMethod, failAbort, null, null);
        }

        private PoolProbeDataSource(DataSource delegate, String failingMethod, boolean failAbort, Throwable injectedFailure) {
            this(delegate, failingMethod, failAbort, injectedFailure, null);
        }

        private PoolProbeDataSource(DataSource delegate,
                                    String failingMethod,
                                    boolean failAbort,
                                    Throwable injectedFailure,
                                    Throwable abortFailure) {
            this.delegate = delegate;
            this.failingMethod = failingMethod;
            this.failAbort = failAbort;
            this.injectedFailure = injectedFailure;
            this.abortFailure = abortFailure;
        }

        int abortCalls() {
            return abortCalls.get();
        }

        int closeCalls() {
            return closeCalls.get();
        }

        int rollbackCalls() {
            return rollbackCalls.get();
        }

        int restoreAutoCommitCalls() {
            return restoreAutoCommitCalls.get();
        }

        @Override
        public Connection getConnection() throws SQLException {
            return connection(delegate.getConnection());
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return connection(delegate.getConnection(username, password));
        }

        @Override public PrintWriter getLogWriter() throws SQLException { return delegate.getLogWriter(); }
        @Override public void setLogWriter(PrintWriter out) throws SQLException { delegate.setLogWriter(out); }
        @Override public void setLoginTimeout(int seconds) throws SQLException { delegate.setLoginTimeout(seconds); }
        @Override public int getLoginTimeout() throws SQLException { return delegate.getLoginTimeout(); }
        @Override public Logger getParentLogger() { return Logger.getGlobal(); }
        @Override public <T> T unwrap(Class<T> type) throws SQLException { return delegate.unwrap(type); }
        @Override public boolean isWrapperFor(Class<?> type) throws SQLException { return delegate.isWrapperFor(type); }

        private Connection connection(Connection connection) {
            return (Connection) Proxy.newProxyInstance(JdbcBatchWriterTest.class.getClassLoader(), new Class[]{Connection.class},
                    (proxy, method, arguments) -> {
                        if (method.getName().equals("rollback")) {
                            rollbackCalls.incrementAndGet();
                        }
                        if (method.getName().equals("setAutoCommit") && Boolean.TRUE.equals(arguments[0])) {
                            restoreAutoCommitCalls.incrementAndGet();
                        }
                        boolean restoreFailure = "restoreAutoCommit".equals(failingMethod)
                                && method.getName().equals("setAutoCommit")
                                && Boolean.TRUE.equals(arguments[0]);
                        boolean disableFailure = "disableAutoCommit".equals(failingMethod)
                                && method.getName().equals("setAutoCommit")
                                && Boolean.FALSE.equals(arguments[0]);
                        boolean partialDisableFailure = "partialDisableAutoCommit".equals(failingMethod)
                                && method.getName().equals("setAutoCommit")
                                && Boolean.FALSE.equals(arguments[0]);
                        if (partialDisableFailure) {
                            try {
                                method.invoke(connection, arguments);
                            } catch (InvocationTargetException error) {
                                throw error.getCause();
                            }
                        }
                        if (method.getName().equals(failingMethod) || restoreFailure || disableFailure
                                || partialDisableFailure) {
                            if (injectedFailure != null) {
                                throw injectedFailure;
                            }
                            throw new SQLException("simulated lost " + failingMethod + " reply", "08006");
                        }
                        if (method.getName().equals("abort")) {
                            abortCalls.incrementAndGet();
                            if (abortFailure != null) {
                                throw abortFailure;
                            }
                            if (failAbort) {
                                throw new SQLException("simulated abort failure", "08006");
                            }
                            return null;
                        }
                        if (method.getName().equals("close")) {
                            closeCalls.incrementAndGet();
                        }
                        try {
                            return method.invoke(connection, arguments);
                        } catch (InvocationTargetException error) {
                            throw error.getCause();
                        }
                    });
        }
    }

    private static final class TrackingPublisher implements Publisher<Object[]> {

        private final List<Object[]> rows;
        private final AtomicBoolean subscribed = new AtomicBoolean();
        private long largestRequest;

        private TrackingPublisher(List<Object[]> rows) {
            this.rows = List.copyOf(rows);
        }

        @Override
        public void subscribe(Subscriber<? super Object[]> subscriber) {
            subscribed.set(true);
            subscriber.onSubscribe(new Subscription() {
                private int index;
                private boolean cancelled;

                @Override
                public void request(long requested) {
                    largestRequest = Math.max(largestRequest, requested);
                    while (!cancelled && requested-- > 0 && index < rows.size()) {
                        subscriber.onNext(rows.get(index++));
                    }
                    if (!cancelled && index == rows.size()) {
                        subscriber.onComplete();
                    }
                }

                @Override
                public void cancel() {
                    cancelled = true;
                }
            });
        }

        boolean subscribed() {
            return subscribed.get();
        }

        long largestRequest() {
            return largestRequest;
        }
    }

    private static final class RecordingBatchObserver implements BatchExecutionObserver {

        private final java.util.ArrayList<BatchExecutionObservation> events = new java.util.ArrayList<>();
        private final java.util.ArrayList<SqlTransactionSource> sources = new java.util.ArrayList<>();

        @Override
        public void onExecution(BatchExecutionObservation observation) {
            events.add(observation);
        }

        @Override
        public void onExecution(BatchExecutionObservation observation, SqlTransactionSource source) {
            events.add(observation);
            sources.add(source);
        }
    }

    private static final class CleanupProbe implements BatchExecutionObserver, SqlExecutionObserver {

        private final List<ResourceCleanupObservation> events = new ArrayList<>();
        private final Throwable callbackFailure;

        private CleanupProbe() {
            this(null);
        }

        private CleanupProbe(Throwable callbackFailure) {
            this.callbackFailure = callbackFailure;
        }

        @Override
        public void onExecution(BatchExecutionObservation observation) {
            // 本夹具只验证批量连接清理事件。
        }

        @Override
        public void onExecution(SqlExecutionObservation observation) {
            // JdbcBatchWriter 不应借清理桥重复发布普通 SQL 事件。
        }

        @Override
        public void onResourceCleanup(ResourceCleanupObservation observation) {
            events.add(observation);
            if (callbackFailure instanceof RuntimeException runtimeFailure) {
                throw runtimeFailure;
            }
            if (callbackFailure instanceof Error fatal) {
                throw fatal;
            }
        }

        private ResourceCleanupObservation singleEvent() {
            assertEquals(1, events.size());
            return events.getFirst();
        }
    }
}
