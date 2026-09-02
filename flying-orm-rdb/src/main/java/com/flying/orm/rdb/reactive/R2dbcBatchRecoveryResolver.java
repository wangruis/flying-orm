package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 根据恢复令牌查询批量回执，并发布恢复观测事件。
 *
 * <p>查到完成回执才能确认 COMMITTED；查不到只表示当前没有提交证据，仍然返回 UNKNOWN，不能擅自推断为回滚。
 * 查询异常继续使用统一数据库错误分类，同时观测事件只记录令牌和结果，不复制批量参数。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class R2dbcBatchRecoveryResolver {

    private final BatchReceiptStore receiptStore;

    private final ReactiveSqlExecutionObservationSupport observationSupport;

    R2dbcBatchRecoveryResolver(BatchReceiptStore receiptStore,
                               ReactiveSqlExecutionObservationSupport observationSupport) {
        this.receiptStore = Objects.requireNonNull(receiptStore, "batch receipt store must not be null");
        this.observationSupport = Objects.requireNonNull(
                observationSupport, "sql execution observation support must not be null");
    }

    Mono<BatchResolution> resolveUnknown(BatchChunkResult.RecoveryToken token) {
        BatchChunkResult.RecoveryToken safeToken = Objects.requireNonNull(token,
                                                                          "batch recovery token must not be null");
        return Mono.defer(() -> {
            long startedAt = System.nanoTime();
            return receiptStore.find(safeToken)
                              .map(ignored -> BatchResolution.committed(safeToken))
                              .switchIfEmpty(Mono.just(BatchResolution.unknown(safeToken)))
                              .doOnSuccess(resolution -> observationSupport.observeRecovery(BatchExecutionObservation.recovery(
                                      resolution,
                                      System.nanoTime() - startedAt,
                                      null)))
                              .onErrorMap(ReactiveSqlExecutionProtection::translate)
                              .doOnError(error -> {
                                  BatchResolution resolution = BatchResolution.unknown(safeToken);
                                  observationSupport.observeRecovery(BatchExecutionObservation.recovery(
                                          resolution,
                                          System.nanoTime() - startedAt,
                                          error));
                              });
        });
    }
}
