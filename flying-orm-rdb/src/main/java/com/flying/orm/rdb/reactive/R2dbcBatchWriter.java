package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.isolation.R2dbcConnectionInvalidator;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * R2DBC 批量写入的内部稳定门面。
 *
 * <p>门面只负责检查请求模式并选择 ATOMIC 或 INDEPENDENT 协调器。分片、参数绑定、连接清理、事务状态、
 * 结果拼装和回执身份各自只有一份实现，避免两种模式在安全边界上逐渐产生不同语义。</p>
 *
 * <p>本对象和内部协作者都不保存请求级状态，可以被多个订阅并发复用。每次订阅的截止时间、分片列表、
 * 事务状态和结果集合都在 Reactor 链内部创建。</p>
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
final class R2dbcBatchWriter {

    private final R2dbcAtomicBatchWriter atomicWriter;
    private final R2dbcIndependentBatchWriter independentWriter;
    private final R2dbcBatchConnectionLifecycle connections;

    R2dbcBatchWriter(ConnectionFactory connectionFactory,
                     BatchReceiptStore receiptStore,
                      R2dbcBindMarkers bindMarkers,
                      SqlExecutionObserver cleanupObserver,
                      BatchExecutionObserver batchObserver,
                      R2dbcConnectionInvalidator connectionInvalidator,
                     R2dbcTransactionParticipant transactionParticipant) {
        R2dbcBatchWriterChunks chunks = new R2dbcBatchWriterChunks(bindMarkers);
        R2dbcBatchReceiptSupport receipts = new R2dbcBatchReceiptSupport();
        this.connections = new R2dbcBatchConnectionLifecycle(
                connectionFactory, cleanupObserver, connectionInvalidator, transactionParticipant);
        R2dbcBatchResultAssembler results = new R2dbcBatchResultAssembler();
        R2dbcExternalBatchCompletion externalCompletion = new R2dbcExternalBatchCompletion(results, batchObserver);
        this.atomicWriter = new R2dbcAtomicBatchWriter(
                chunks, receiptStore, receipts, connections, results, externalCompletion);
        this.independentWriter = new R2dbcIndependentBatchWriter(
                chunks, receiptStore, receipts, connections, results);
    }

    Mono<BatchWriteResult> write(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        Mono<BatchWriteResult> execution = safeRequest.options().mode() == BatchWriteOptions.Mode.INDEPENDENT
                ? independentWriter.write(safeRequest) : atomicWriter.write(safeRequest);
        // 校验在获取连接、订阅输入和执行第一条 SQL 之前完成。
        return connections.validate(safeRequest.options()).then(execution);
    }

    Flux<BatchChunkResult> writeChunks(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        return connections.validate(safeRequest.options()).thenMany(independentWriter.writeChunks(safeRequest));
    }
}
