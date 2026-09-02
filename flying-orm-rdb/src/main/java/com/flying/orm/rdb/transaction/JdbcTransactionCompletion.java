package com.flying.orm.rdb.transaction;

import org.reactivestreams.Publisher;

/**
 * 上层 JDBC 事务结束通知的框架无关注册入口。
 *
 * <p>flying-orm 只能登记回调，不能通过这个接口提交、回滚或关闭连接。Spring、Jakarta CDI 或自研容器
 * 应把监听器挂到自己的事务同步机制上，并在物理事务结束后执行监听器返回的 Publisher。返回值使用
 * Reactive Streams 只是为了复用批量生命周期契约，不会让 JDBC SQL 执行转成 R2DBC。</p>
 *
 * <p>返回 {@code false} 表示适配器只能提供事务连接，不能提供最终完成通知。SQL 仍然可以加入事务并
 * 返回 ENLISTED，但 ORM 会把无法确认的后续协作按 UNKNOWN 收尾，不能假装事务已经提交。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
@FunctionalInterface
public interface JdbcTransactionCompletion {

    /**
     * 登记一次事务结束监听。实现方不得在登记阶段提前执行监听器。
     *
     * @param listener 事务真正结束后才调用的监听器
     * @return 已可靠登记返回 true；当前适配器不支持完成通知返回 false
     */
    boolean register(Listener listener);

    /** 返回不提供完成通知的默认实现，供只有事务连接的简单适配器使用。 */
    static JdbcTransactionCompletion unavailable() {
        return ignored -> false;
    }

    /** 上层完成事务后调用，返回的协作流由上层事务适配器纳入自己的完成流程。 */
    @FunctionalInterface
    interface Listener {
        Publisher<Void> afterCompletion(TransactionOutcome outcome);
    }
}
