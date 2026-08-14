package com.flying.orm.rdb.transaction;

import java.util.Objects;
import java.util.Optional;

/**
 * 让任意 Java 容器把当前 JDBC 事务连接交给 flying-orm 的框架无关入口。
 *
 * <p>没有外部事务时返回空，执行器会从 DataSource 借一条连接并在调用结束后归还。有外部事务时返回
 * {@link JdbcTransactionContext}，本次 SQL 会直接复用其中的连接，并且绝不会提交、回滚或关闭它。
 * Spring、Jakarta CDI 或自研容器只需要在自己的事务上下文中实现一次这个接口。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
@FunctionalInterface
public interface JdbcTransactionParticipant {

    /** 返回当前调用线程已经绑定的事务；没有事务时返回空。 */
    Optional<JdbcTransactionContext> currentTransaction();

    /**
     * 返回本次 JDBC 调用希望使用的物理路由标识。普通单数据源保持默认 {@code null}；动态数据源适配器
     * 应返回当前线程已经选择的库，让 ORM 在执行 SQL 前和事务锁定值比较。
     */
    default String currentRoutingIdentity() {
        return null;
    }

    /** 供 JDBC 执行链统一读取事务，确保所有入口都经过路由一致性校验。 */
    default Optional<JdbcTransactionContext> currentTransactionForExecution() {
        return currentTransaction(currentRoutingIdentity());
    }

    /**
     * 读取当前事务并核对调用方要求的路由。事务开始后不能换库，因此不一致时必须在执行 SQL 前失败。
     *
     * @param requestedRoutingIdentity 本次调用指定的路由；{@code null} 表示没有额外指定
     */
    default Optional<JdbcTransactionContext> currentTransaction(String requestedRoutingIdentity) {
        String requested = requestedRoutingIdentity == null ? null : requestedRoutingIdentity.trim();
        if (requested != null && requested.isEmpty()) {
            throw new IllegalArgumentException("requested routing identity must not be blank");
        }
        Optional<JdbcTransactionContext> transaction = Objects.requireNonNull(
                currentTransaction(), "current transaction must not be null");
        if (requested == null || transaction.isEmpty()) {
            return transaction;
        }
        if (!requested.equals(transaction.get().routingIdentity())) {
            throw new IllegalStateException("jdbc transaction routing identity cannot change after transaction start");
        }
        return transaction;
    }

    /** 默认参与者表示当前没有外部 JDBC 事务。 */
    static JdbcTransactionParticipant none() {
        return Optional::empty;
    }
}
