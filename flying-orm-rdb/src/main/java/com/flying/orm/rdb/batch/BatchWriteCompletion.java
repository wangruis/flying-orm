package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.internal.InternalApi;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * 外部事务真正结束后需要执行的批量协作。
 *
 * <p>普通调用不需要设置它。Repository 用它把实体 after 生命周期延迟到外部事务确认提交之后；
 * 回调拿到的已经是 COMMITTED、ROLLED_BACK 或 UNKNOWN 最终结果，不会拿 ENLISTED 猜测提交结局。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
@FunctionalInterface
public interface BatchWriteCompletion {

    /** @param result 外部事务结束后的最终批量结果；@return 由事务适配器执行的响应式协作。 */
    Publisher<Void> afterCompletion(BatchWriteResult result);

    /**
     * 完成通知无法注册时，同步释放实现内部保留的状态。
     *
     * <p>该内部钩子只用于 JDBC 适配器的 UNKNOWN 收尾，避免为清理状态而订阅任意
     * {@link #afterCompletion(BatchWriteResult)} Publisher。普通实现无需覆盖。</p>
     *
     * @param result 结果未知的批量完成信号
     */
    @InternalApi
    default void afterCompletionUnavailable(BatchWriteResult result) {
        // 默认实现没有需要同步释放的内部状态。
    }

    /** 返回没有提交后协作的默认实现。 */
    static BatchWriteCompletion noop() {
        return ignored -> Mono.empty();
    }
}
