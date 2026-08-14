package com.flying.orm.rdb.transaction;

import io.r2dbc.spi.Connection;

import java.util.Objects;

/**
 * 一次已经开始的外部事务交给 flying-orm 使用时需要提供的最小事实。
 *
 * <p>这个对象不是事务管理器：它不会开始、提交、回滚或关闭连接。它只声明三件事情：当前连接由谁持有、
 * 事务来自哪里，以及路由在事务期间已经锁定到哪个物理目标。ORM 因而可以执行 SQL，却不会越过上层的
 * 事务边界做资源管理。</p>
 *
 * <p>外部适配器必须从主库取得这条事务连接；事务中的查询和写入都会复用它，不再调用可能选择从库的连接工厂。
 * {@code routingIdentity} 使用与 {@code IsolationContext.databaseKey} 相同的稳定数据库键，默认库没有显式键时
 * 仍需给出便于诊断的非空身份。</p>
 *
 * @param connection      外部事务已经绑定的连接
 * @param ownership       连接所有权，外部事务只能由调用方持有
 * @param origin          事务来源，当前契约只接纳外部事务
 * @param routingIdentity 已锁定的物理数据源或路由身份，不能是空白文本
 * @param completion      外层事务结束通知注册器
 * @author wangr
 * @version v1.0
 */
public record R2dbcTransactionContext(Connection connection,
                                      ConnectionOwnership ownership,
                                      TransactionOrigin origin,
                                      String routingIdentity,
                                      R2dbcTransactionCompletion completion) {

    /** 兼容只提供外部连接的适配器；这类上下文可以参与事务，但没有最终完成通知。 */
    public R2dbcTransactionContext(Connection connection,
                                   ConnectionOwnership ownership,
                                   TransactionOrigin origin,
                                   String routingIdentity) {
        this(connection, ownership, origin, routingIdentity, R2dbcTransactionCompletion.unavailable());
    }

    /**
     * 校验外部事务不能伪装成 ORM 自管连接。路由身份会随这个不可变上下文在同一响应式订阅中传播，
     * 因此事务开始后 ORM 不会再次选择数据源。
     */
    public R2dbcTransactionContext {
        connection = Objects.requireNonNull(connection, "transaction connection must not be null");
        ownership = Objects.requireNonNull(ownership, "transaction connection ownership must not be null");
        origin = Objects.requireNonNull(origin, "transaction origin must not be null");
        completion = Objects.requireNonNull(completion, "transaction completion must not be null");
        routingIdentity = Objects.requireNonNull(routingIdentity, "transaction routing identity must not be null").trim();
        if (routingIdentity.isEmpty()) {
            throw new IllegalArgumentException("transaction routing identity must not be blank");
        }
        if (ownership != ConnectionOwnership.EXTERNAL || origin != TransactionOrigin.EXTERNAL_TRANSACTION) {
            throw new IllegalArgumentException("only externally owned external transactions can participate");
        }
    }

    /**
     * 用外部事务管理器已经绑定的主库连接创建上下文。调用方负责让同一事务内的所有订阅看到同一个上下文，
     * 并在事务结束后自行提交、回滚和归还连接。
     */
    public static R2dbcTransactionContext external(Connection connection, String routingIdentity) {
        return new R2dbcTransactionContext(connection,
                                           ConnectionOwnership.EXTERNAL,
                                           TransactionOrigin.EXTERNAL_TRANSACTION,
                                           routingIdentity,
                                           R2dbcTransactionCompletion.unavailable());
    }

    /** 用外部连接和真正的事务结束通知创建上下文，完整适配器应优先使用这个入口。 */
    public static R2dbcTransactionContext external(Connection connection,
                                                    String routingIdentity,
                                                    R2dbcTransactionCompletion completion) {
        return new R2dbcTransactionContext(connection,
                                           ConnectionOwnership.EXTERNAL,
                                           TransactionOrigin.EXTERNAL_TRANSACTION,
                                           routingIdentity,
                                           Objects.requireNonNull(
                                                   completion, "transaction completion must not be null"));
    }

    /** 连接仍由外部事务管理器持有，flying-orm 只能使用，不能结束它。 */
    public boolean externallyManaged() {
        return ownership == ConnectionOwnership.EXTERNAL;
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
