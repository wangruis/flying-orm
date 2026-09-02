package com.flying.orm.rdb.transaction;

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

    /** 默认参与者表示当前没有外部 JDBC 事务。 */
    static JdbcTransactionParticipant none() {
        return Optional::empty;
    }
}
