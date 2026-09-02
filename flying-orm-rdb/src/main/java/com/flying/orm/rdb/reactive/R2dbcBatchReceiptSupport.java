package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * 批量回执所需的计划摘要、数据摘要和恢复令牌生成规则。
 *
 * <p>这个类型只计算身份，不读写回执表，也不决定事务提交。ATOMIC 和 INDEPENDENT 因而使用完全相同的
 * 摘要协议；计划身份包含 SQL、绑定类型、影响行数策略和分片规则，数据身份按原始输入行顺序计算。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class R2dbcBatchReceiptSupport {

    private final BatchPayloadHasher payloadHasher = new BatchPayloadHasher();

    String planHash(BatchWriteRequest request) {
        return payloadHasher.hashPlan(Objects.requireNonNull(request, "batch write request must not be null"));
    }

    String chunkPayloadHash(R2dbcBatchWriterChunks.BatchChunk chunk) {
        R2dbcBatchWriterChunks.BatchChunk safeChunk = Objects.requireNonNull(
                chunk, "batch chunk must not be null");
        return payloadHasher.hashRowViews(safeChunk.rows());
    }

    BatchReceiptDigest newPayloadDigest() {
        return payloadHasher.newPayloadEncoder();
    }

    void updatePayload(BatchReceiptDigest digest, R2dbcBatchWriterChunks.BatchChunk chunk) {
        Objects.requireNonNull(chunk, "batch chunk must not be null");
        // 摘要只看原始行顺序，不包含分片边界；调整 chunkSize 不会改变同一批数据的身份。
        for (ProtectedBatchRows.RowView row : chunk.rows()) {
            payloadHasher.updateRow(digest, row);
        }
    }

    String finishPayload(BatchReceiptDigest digest) {
        return payloadHasher.finish(digest);
    }

    /** 已提交回执重放不持有事务连接，输入等待由 Publisher 或上层负责，不能复用批量 SQL 截止时间。 */
    Mono<String> hashPayload(BatchWriteRequest request,
                             R2dbcBatchWriterChunks chunks,
                             LongConsumer acceptedRows) {
        BatchReceiptDigest digest = newPayloadDigest();
        return chunks.chunks(request, Objects.requireNonNull(
                             acceptedRows, "accepted row tracker must not be null"))
                     .doOnNext(chunk -> updatePayload(digest, chunk))
                     .then(Mono.fromSupplier(() -> finishPayload(digest)));
    }

    BatchChunkResult.RecoveryToken recoveryToken(BatchWriteRequest request,
                                                  int chunkIndex,
                                                  String planHash,
                                                  String payloadHash,
                                                  Long expectedRowCount,
                                                  Long expectedAffectedRows) {
        BatchWriteOptions.Recovery recovery = Objects.requireNonNull(request,
                                                                     "batch write request must not be null")
                                                             .options()
                                                             .recovery();
        return new BatchChunkResult.RecoveryToken(recovery.operationId(),
                                                  chunkIndex,
                                                  recovery.receiptTable(),
                                                  planHash,
                                                  payloadHash,
                                                  expectedRowCount,
                                                  expectedAffectedRows);
    }
}
