package com.flying.orm.rdb.reactive;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchReceiptIntegrityException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.isolation.R2dbcConnectionInvalidator;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/**
 * BatchReceiptStore 读写批量回执表，UNKNOWN 恢复只相信这里能确认到的事实。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
final class BatchReceiptStore {

    private static final ValueCodecRegistry VALUE_CODECS = ValueCodecRegistry.standard();

    private final ConnectionFactory connectionFactory;

    private final R2dbcBindMarkers bindMarkers;
    private final SqlExecutionObserver observer;
    private final R2dbcConnectionInvalidator connectionInvalidator;

    BatchReceiptStore(ConnectionFactory connectionFactory,
                      R2dbcBindMarkers bindMarkers,
                      SqlExecutionObserver observer,
                      R2dbcConnectionInvalidator connectionInvalidator) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connection factory must not be null");
        this.bindMarkers = Objects.requireNonNull(bindMarkers, "r2dbc bind markers must not be null");
        this.observer = Objects.requireNonNull(observer, "SQL execution observer must not be null");
        this.connectionInvalidator = Objects.requireNonNull(connectionInvalidator,
                                                             "connection invalidator must not be null");
    }

    /**
     * 查询完全匹配的已提交回执。查不到只能说明暂时不能确认，不能推断为已回滚。
     *
     * @param token 恢复令牌
     * @return 回执内容
     */
    Mono<Receipt> find(BatchChunkResult.RecoveryToken token) {
        return find(token, SqlExecutionOptions.DEFAULT_TIMEOUT);
    }

    /** 查询连接由连接池决定等待时间；连接可用后，SQL 可选择独立的执行兜底时限。 */
    Mono<Receipt> find(BatchChunkResult.RecoveryToken token, Duration timeout) {
        BatchChunkResult.RecoveryToken safeToken = Objects.requireNonNull(token,
                                                                          "batch recovery token must not be null");
        Duration safeTimeout = requireTimeout(timeout);
        if (!safeToken.hasCompleteEvidence()) {
            // 只有 operation id/plan hash 不能证明流式输入已经完整写入；恢复入口必须保持 UNKNOWN。
            return Mono.empty();
        }
        String sql = "select row_count, affected_rows from " + receiptTable(safeToken.receiptTable())
                + " where operation_id = ? and chunk_index = ? and plan_hash = ? and payload_hash = ?"
                + " and status = 'COMMITTED'";
        Flux<Receipt> receipts = Flux.usingWhen(acquireConnection(),
                                                connection -> protectRead(
                                                        Flux.from(bind(connection.createStatement(sqlForDriver(sql, 4)),
                                                                       List.of(safeToken.operationId(),
                                                                               safeToken.chunkIndex(),
                                                                               safeToken.planHash(),
                                                                               safeToken.payloadHash())).execute())
                                                            .flatMap(result -> result.map(this::receipt)),
                                                        safeTimeout),
                                                this::closeAfterConfirmedRead,
                                                this::invalidateAfterUnconfirmedRead,
                                                connection -> invalidateAfterUnconfirmedRead(
                                                        connection,
                                                        new CancellationException(
                                                                "batch receipt read cancelled before completion")));
        return zeroOrOne(receipts, "find").map(receipt -> requireMatching(safeToken, receipt));
    }

    /**
     * 先按稳定操作编号和执行计划查已完成回执，payload 摘要由调用方流式算完后再比对。
     * 这个入口让安全重试不必先把所有参数行收进 List。
     */
    Mono<Receipt> findOperation(BatchWriteOptions.Recovery recovery,
                                int chunkIndex,
                                String planHash) {
        return findOperation(recovery, chunkIndex, planHash, Duration.ZERO);
    }

    /** 回执连接排队不计入 SQL 执行时限。 */
    Mono<Receipt> findOperation(BatchWriteOptions.Recovery recovery,
                                int chunkIndex,
                                String planHash,
                                Duration timeout) {
        BatchWriteOptions.Recovery safeRecovery = Objects.requireNonNull(recovery,
                                                                          "batch recovery must not be null");
        return findOperation(safeRecovery.operationId(),
                             safeRecovery.receiptTable(),
                             chunkIndex,
                             planHash,
                             requireTimeout(timeout));
    }

    private Mono<Receipt> findOperation(String operationId,
                                        String receiptTable,
                                        int chunkIndex,
                                        String planHash,
                                        Duration timeout) {
        String sql = "select payload_hash, row_count, affected_rows from " + receiptTable(receiptTable)
                + " where operation_id = ? and chunk_index = ? and plan_hash = ? and status = 'COMMITTED'";
        Flux<Receipt> receipts = Flux.usingWhen(acquireConnection(),
                                                connection -> protectRead(
                                                        Flux.from(bind(connection.createStatement(sqlForDriver(sql, 3)),
                                                                       List.of(operationId,
                                                                               chunkIndex,
                                                                               planHash)).execute())
                                                            .flatMap(result -> result.map(this::receiptWithPayload)),
                                                        timeout),
                                                this::closeAfterConfirmedRead,
                                                this::invalidateAfterUnconfirmedRead,
                                                connection -> invalidateAfterUnconfirmedRead(
                                                        connection,
                                                        new CancellationException(
                                                                "batch receipt read cancelled before completion")));
        return zeroOrOne(receipts, "find operation");
    }

    /**
     * 在业务事务里先占住操作编号，避免同一编号同时写两份业务数据。
     *
     * @param connection  数据库连接
     * @param recovery    回执配置
     * @param chunkIndex  分片编号
     * @param planHash    SQL 计划摘要
     * @return 完成信号
     */
    Mono<Void> reserve(Connection connection,
                       BatchWriteOptions.Recovery recovery,
                       int chunkIndex,
                       String planHash) {
        BatchWriteOptions.Recovery safeRecovery = Objects.requireNonNull(recovery, "batch recovery must not be null");
        // Oracle 会把空字符串当成 NULL。预留回执还没有真实参数摘要，但这里必须先写一个四库都能保存的非空值；
        // 同一事务提交前，complete 会把它替换成真正的 payload hash，恢复查询也只读取 COMMITTED 回执。
        String sql = "insert into " + receiptTable(safeRecovery.receiptTable())
                + " (operation_id, chunk_index, plan_hash, payload_hash, row_count, affected_rows, status, created_at)"
                + " values (?, ?, ?, 'RESERVED', 0, 0, 'RESERVED', current_timestamp)";
        return rowsUpdated(connection,
                           sql,
                           List.of(safeRecovery.operationId(), chunkIndex, planHash))
                .map(rows -> requireExactlyOne("reserve", rows))
                .then();
    }

    /**
     * 提交前补完整回执。业务数据和回执一起提交，后面才能用它确认 UNKNOWN。
     *
     * @param connection   数据库连接
     * @param recovery     回执配置
     * @param chunkIndex   分片编号
     * @param payloadHash  参数摘要
     * @param rowCount     输入行数
     * @param affectedRows 影响行数
     * @return 完成信号
     */
    Mono<Void> complete(Connection connection,
                        BatchWriteOptions.Recovery recovery,
                        int chunkIndex,
                        String payloadHash,
                        long rowCount,
                        long affectedRows) {
        BatchWriteOptions.Recovery safeRecovery = Objects.requireNonNull(recovery, "batch recovery must not be null");
        String sql = "update " + receiptTable(safeRecovery.receiptTable())
                + " set payload_hash = ?, row_count = ?, affected_rows = ?, status = 'COMMITTED'"
                + " where operation_id = ? and chunk_index = ?";
        return rowsUpdated(connection,
                           sql,
                           List.of(payloadHash,
                                   rowCount,
                                   affectedRows,
                                   safeRecovery.operationId(),
                                   chunkIndex))
                .map(rows -> requireExactlyOne("complete", rows))
                .then();
    }

    /** 回执写入只能命中唯一事实行。 */
    static long requireExactlyOne(String operation, long rows) {
        if (rows != 1L) {
            throw new BatchReceiptIntegrityException(operation, rows);
        }
        return rows;
    }

    /** 回执读取允许没有结果，但重复结果表示唯一性事实已经损坏。 */
    static <T> Mono<T> zeroOrOne(Flux<T> rows, String operation) {
        return rows.take(2)
                   .collectList()
                   .flatMap(values -> {
                       if (values.size() > 1) {
                           return Mono.error(new BatchReceiptIntegrityException(operation,
                                                                                "return at most one row",
                                                                                values.size()));
                       }
                       return values.isEmpty() ? Mono.empty() : Mono.just(values.getFirst());
                   });
    }

    private Mono<Long> rowsUpdated(Connection connection, String sql, List<Object> parameters) {
        Statement statement = bind(connection.createStatement(sqlForDriver(sql, parameters.size())), parameters);
        return Flux.from(statement.execute())
                   .flatMap(Result::getRowsUpdated)
                   .reduce(0L, R2dbcExecutionCounts::add);
    }

    private Statement bind(Statement statement, List<Object> parameters) {
        for (int i = 0; i < parameters.size(); i++) {
            statement.bind(i, parameters.get(i));
        }
        return statement;
    }

    private Receipt receipt(Row row, RowMetadata ignored) {
        return new Receipt("", exactLong(row, 0, "row_count"), exactLong(row, 1, "affected_rows"));
    }

    private Receipt receiptWithPayload(Row row, RowMetadata ignored) {
        String payloadHash = Objects.requireNonNull(row.get(0, String.class),
                                                     "batch receipt payload_hash must not be null");
        return new Receipt(payloadHash,
                           exactLong(row, 1, "row_count"),
                           exactLong(row, 2, "affected_rows"));
    }

    static long exactLong(Row row, int index, String column) {
        Object value = Objects.requireNonNull(row.get(index), "batch receipt " + column + " must not be null");
        try {
            // UNKNOWN 恢复会把回执当成已经提交的事实。这里不能直接调用 Number.longValue()，
            // 否则超范围和带小数的驱动值会被静默截断，最终拼出一份看似成功但计数错误的结果。
            long exact = VALUE_CODECS.read(value, Long.class);
            if (exact < 0L) {
                throw new BatchReceiptIntegrityException(column, "be non-negative", exact);
            }
            return exact;
        } catch (BatchReceiptIntegrityException error) {
            throw error;
        } catch (IllegalArgumentException error) {
            throw new BatchReceiptIntegrityException(column, "is not an exact long", error);
        }
    }

    /**
     * 将回执计数与恢复令牌里的完整预期事实逐项比对。
     *
     * <p>SQL 的 operation、chunk、plan 和 payload 条件已经负责身份匹配；这里继续验证数据库返回的行数，
     * 防止损坏或被错误修改的回执把 UNKNOWN 提升成 COMMITTED。影响行数允许按策略留空，但一旦令牌携带
     * 预期值就必须精确一致。</p>
     */
    static Receipt requireMatching(BatchChunkResult.RecoveryToken token, Receipt receipt) {
        BatchChunkResult.RecoveryToken safeToken = Objects.requireNonNull(token,
                                                                          "batch recovery token must not be null");
        Receipt safeReceipt = Objects.requireNonNull(receipt, "batch receipt must not be null");
        if (!safeToken.hasCompleteEvidence()) {
            throw new BatchReceiptIntegrityException("resolve", "requires complete recovery evidence",
                                                     new IllegalArgumentException("payload hash or row count is missing"));
        }
        if (safeReceipt.rowCount() < 0L) {
            throw new BatchReceiptIntegrityException("row_count", "be non-negative", safeReceipt.rowCount());
        }
        if (safeReceipt.affectedRows() < 0L) {
            throw new BatchReceiptIntegrityException("affected_rows", "be non-negative", safeReceipt.affectedRows());
        }
        if (!safeReceipt.payloadHash().isEmpty()
                && !safeReceipt.payloadHash().equals(safeToken.payloadHash())) {
            throw new BatchReceiptIntegrityException("payload_hash", "match complete recovery evidence",
                                                     new IllegalStateException("payload hash mismatch"));
        }
        if (safeReceipt.rowCount() != safeToken.expectedRowCount()) {
            throw new BatchReceiptIntegrityException("row_count",
                                                     "match expected row count " + safeToken.expectedRowCount(),
                                                     safeReceipt.rowCount());
        }
        Long expectedAffectedRows = safeToken.expectedAffectedRows();
        if (expectedAffectedRows != null && safeReceipt.affectedRows() != expectedAffectedRows) {
            throw new BatchReceiptIntegrityException("affected_rows",
                                                     "match expected affected rows " + expectedAffectedRows,
                                                     safeReceipt.affectedRows());
        }
        return safeReceipt;
    }

    private String sqlForDriver(String sql, int parameterCount) {
        return bindMarkers.adapt(sql, parameterCount, SqlBindMarkerStyle.CANONICAL);
    }

    private String receiptTable(String table) {
        return bindMarkers.identifier(table);
    }

    private Mono<Connection> acquireConnection() {
        // 排队和获取超时由上层连接池负责；SQL 时限只在连接可用后保护回执查询。
        return Mono.defer(() -> Mono.from(connectionFactory.create()));
    }

    private static <T> Flux<T> protectRead(Flux<T> source, Duration timeout) {
        return timeout.isZero() ? source : SqlExecutionTimeouts.absolute(source, timeout);
    }

    private static Duration requireTimeout(Duration timeout) {
        Duration safeTimeout = Objects.requireNonNull(timeout, "receipt SQL timeout must not be null");
        if (safeTimeout.isNegative()) {
            throw new IllegalArgumentException("receipt SQL timeout must not be negative");
        }
        return safeTimeout;
    }

    private Mono<Void> closeAfterConfirmedRead(Connection connection) {
        R2dbcCleanupDeadline deadline = R2dbcCleanupDeadline.start(
                SqlExecutionOptions.DEFAULT_CLEANUP_TIMEOUT);
        return deadline.protect(Mono.defer(() -> Mono.from(connectionInvalidator.close(connection))))
                   .onErrorResume(error -> finishInvalidation(
                           connection, error, ResourceCleanupObservation.Phase.CONNECTION_CLOSE, true, deadline));
    }

    private Mono<Void> invalidateAfterUnconfirmedRead(Connection connection, Throwable cause) {
        return finishInvalidation(
                connection,
                cause,
                ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                false,
                R2dbcCleanupDeadline.start(SqlExecutionOptions.DEFAULT_CLEANUP_TIMEOUT));
    }

    private Mono<Void> finishInvalidation(Connection connection,
                                          Throwable primaryError,
                                          ResourceCleanupObservation.Phase phase,
                                          boolean outcomeConfirmed,
                                          R2dbcCleanupDeadline deadline) {
        Mono<Void> invalidation = deadline.protectInvalidation(
                Mono.defer(() -> Mono.from(connectionInvalidator.invalidate(connection))));
        return invalidation.onErrorResume(invalidationError -> {
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.promoteVirtualMachineError(
                    primaryError, invalidationError);
            if (fatal != null) {
                observeCleanup(phase, outcomeConfirmed, fatal);
                return Mono.error(fatal);
            }
            ReactiveSqlExecutionProtection.addSuppressedIfAcyclic(primaryError, invalidationError);
            return Mono.empty();
        }).then(Mono.defer(() -> {
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(primaryError);
            observeCleanup(phase, outcomeConfirmed, fatal == null ? primaryError : fatal);
            return fatal == null ? Mono.empty() : Mono.error(fatal);
        }));
    }

    private void observeCleanup(ResourceCleanupObservation.Phase phase,
                                boolean outcomeConfirmed,
                                Throwable error) {
        observer.onResourceCleanup(new ResourceCleanupObservation(
                SqlExecutionOperation.QUERY, phase, outcomeConfirmed, error));
    }

    /**
     * 已提交回执里对重建结果有用的字段。
     *
     * @param rowCount     输入行数
     * @param affectedRows 影响行数
     */
    record Receipt(String payloadHash, long rowCount, long affectedRows) {

        /**
         * 将持久化输入行数转换为内存分片模型使用的精确 int，超界时保持回执完整性异常语义。
         *
         * @return 可安全构造 {@code BatchChunkResult} 的输入行数
         */
        int exactInputRowCount() {
            if (rowCount > Integer.MAX_VALUE) {
                throw new BatchReceiptIntegrityException(
                        "row_count", "fit the in-memory chunk capacity", rowCount);
            }
            return (int) rowCount;
        }
    }
}
