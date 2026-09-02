package com.flying.orm.rdb.transaction;

import org.reactivestreams.Publisher;

/**
 * 外部事务结束通知的框架无关注册入口。
 *
 * <p>flying-orm 只能向这里登记回调，不能通过它提交或回滚事务。上层容器适配器
 * 应把回调挂到自己真正的事务同步机制上，并在物理事务结束后执行回调返回的 Publisher。一次成功注册
 * 必须且只能通知一次；事务已经结束时应立即登记到对应的最终状态，不能静默漏掉通知。</p>
 *
 * <p>返回 {@code false} 表示当前适配器没有提供完成通知。此时 SQL 仍可加入外部事务并返回 ENLISTED，
 * 只是 flying-orm 无法继续发出最终事务观测或执行提交后协作。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
@FunctionalInterface
public interface R2dbcTransactionCompletion {

    /**
     * 注册事务结束回调。实现方不得在注册阶段订阅回调 Publisher，也不得让回调异常改变已经确定的事务结果。
     *
     * @param listener 事务真正结束后调用的响应式回调
     * @return 已可靠登记返回 true；不支持完成通知返回 false
     */
    boolean register(Listener listener);

    /** 返回不提供结束通知的默认实现，兼容只会暴露事务连接的简单适配器。 */
    static R2dbcTransactionCompletion unavailable() {
        return ignored -> false;
    }

    /** 回调 Publisher 由外层事务适配器纳入完成流程，flying-orm 不会手动 subscribe 或 block。 */
    @FunctionalInterface
    interface Listener {
        Publisher<Void> afterCompletion(TransactionOutcome outcome);
    }
}
