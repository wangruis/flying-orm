package com.flying.orm.rdb.internal.batch;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.internal.InternalApi;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * 内建批次与 Repository 之间的分片终态通知。
 *
 * <p>同步执行器释放租约后通知并处理 POST。响应式执行器在确认终态时通知，
 * 只记录已提交事实；ATOMIC 必须等整批提交确认后才能通知各片，不能把中途执行成功当作提交。
 * 释放租约后再执行 POST；INDEPENDENT 使用分片释放回调，ATOMIC 使用 Repository 的整批完成处理。
 * 外部事务继续使用 {@link BatchWriteCompletion} 原有完成合同。</p>
 *
 * @author wangr
 * @version v1.0
 */
@InternalApi
public interface BatchChunkCompletion extends BatchWriteCompletion {

    /** 记录分片终态；响应式实现必须无 I/O、可重复通知，不改变事务事实。 */
    void afterChunk(BatchChunkResult result);

    /** 响应式分片释放连接后执行 POST 和实体清理；同步实现已在 afterChunk 中完成。 */
    default Publisher<Void> afterChunkReleased(BatchChunkResult result) {
        return Mono.empty();
    }
}
