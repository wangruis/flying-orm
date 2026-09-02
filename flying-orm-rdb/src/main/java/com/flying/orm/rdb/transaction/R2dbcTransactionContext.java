package com.flying.orm.rdb.transaction;

import io.r2dbc.spi.Connection;

import java.util.Objects;

/**
 * 一次已经开始的外部事务交给 flying-orm 使用时需要提供的最小事实。
 *
 * <p>这个对象不是事务管理器：它不会开始、提交、回滚或关闭连接。它只声明当前连接由谁持有，
 * 以及事务来自哪里。ORM 因而可以执行 SQL，却不会越过上层的
 * 事务边界做资源管理。</p>
 *
 * <p>外部适配器必须先选好数据源并取得事务连接；事务中的查询和写入都会复用它，
 * ORM 不再调用连接工厂，也不参与数据源路由。</p>
 *
 * @param connection      外部事务已经绑定的连接
 * @param ownership       连接所有权，外部事务只能由调用方持有
 * @param origin          事务来源，当前契约只接纳外部事务
 * @param completion      外层事务结束通知注册器
 * @author wangr
 * @version v1.0
 */
public record R2dbcTransactionContext(Connection connection,
                                      ConnectionOwnership ownership,
                                      TransactionOrigin origin,
                                      R2dbcTransactionCompletion completion) {

    /** 兼容只提供外部连接的适配器；这类上下文可以参与事务，但没有最终完成通知。 */
    public R2dbcTransactionContext(Connection connection,
                                   ConnectionOwnership ownership,
                                   TransactionOrigin origin) {
        this(connection, ownership, origin, R2dbcTransactionCompletion.unavailable());
    }

    /** 保留公开构造契约，并冻结外部事务事实。 */
    public R2dbcTransactionContext {
        connection = Objects.requireNonNull(connection, "transaction connection must not be null");
        ownership = Objects.requireNonNull(ownership, "transaction connection ownership must not be null");
        origin = Objects.requireNonNull(origin, "transaction origin must not be null");
        completion = Objects.requireNonNull(completion, "transaction completion must not be null");
    }

    /**
     * 用外部事务管理器已经绑定的主库连接创建上下文。调用方负责让同一事务内的所有订阅看到同一个上下文，
     * 并在事务结束后自行提交、回滚和归还连接。
     */
    public static R2dbcTransactionContext external(Connection connection) {
        return new R2dbcTransactionContext(connection,
                                           ConnectionOwnership.EXTERNAL,
                                           TransactionOrigin.EXTERNAL_TRANSACTION,
                                           R2dbcTransactionCompletion.unavailable());
    }

    /** 用外部连接和真正的事务结束通知创建上下文，完整适配器应优先使用这个入口。 */
    public static R2dbcTransactionContext external(Connection connection,
                                                    R2dbcTransactionCompletion completion) {
        return new R2dbcTransactionContext(connection,
                                           ConnectionOwnership.EXTERNAL,
                                           TransactionOrigin.EXTERNAL_TRANSACTION,
                                           Objects.requireNonNull(
                                                   completion, "transaction completion must not be null"));
    }

    /** 连接仍由外部事务管理器持有，flying-orm 只能使用，不能结束它。 */
    public boolean externallyManaged() {
        return true;
    }

    /** 外部事务参与场景唯一允许的连接所有权。 */
    public enum ConnectionOwnership {
        EXTERNAL
    }

    /** 事务来源会进入执行语义判断，避免把外层事务误当成 ORM 已提交事务。 */
    public enum TransactionOrigin {
        EXTERNAL_TRANSACTION
    }
}
