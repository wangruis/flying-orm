package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import reactor.core.publisher.Mono;

import java.security.MessageDigest;
import java.util.Objects;

/**
 * 批量回执所需的计划摘要、数据摘要和恢复令牌生成规则。
 *
 * <p>这个类型只计算身份，不读写回执表，也不决定事务提交。ATOMIC 和 INDEPENDENT 因而使用完全相同的
 * 摘要协议，修改 chunkSize 之外的请求结构或任意输入值都会得到不同身份。</p>
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
        return payloadHasher.hashRows(Objects.requireNonNull(
                chunk, "batch chunk must not be null").receiptRows());
    }

    MessageDigest newPayloadDigest() {
        return payloadHasher.newDigest();
    }

    void updatePayload(MessageDigest digest, R2dbcBatchWriterChunks.BatchChunk chunk) {
        Objects.requireNonNull(chunk, "batch chunk must not be null");
        // 摘要只看原始行顺序，不包含分片边界；调整 chunkSize 不会改变同一批数据的身份。
        for (Object[] row : chunk.receiptRows()) {
            payloadHasher.updateRow(digest, row);
        }
    }

    String finishPayload(MessageDigest digest) {
        return payloadHasher.finish(digest);
    }

    Mono<String> hashPayload(BatchWriteRequest request,
                             R2dbcBatchDeadline deadline,
                             R2dbcBatchWriterChunks chunks) {
        MessageDigest digest = newPayloadDigest();
        return deadline.protect(chunks.chunks(request))
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
