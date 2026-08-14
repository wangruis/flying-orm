package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import io.r2dbc.spi.Connection;

import java.util.Objects;

/**
 * 保存批量事务使用的连接和事务状态。
 *
 * <p>对象只保存连接生命周期需要共享的最小状态。状态字段使用 volatile，因为取消回调和主执行链可能不在
 * 同一线程；真正的事务操作仍由 Reactor 链按顺序串行执行。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class R2dbcBatchConnectionHandle {

    private final Connection connection;

    /** 外部事务句柄保留完整上下文；ORM 自管连接时为空。 */
    private final R2dbcTransactionContext externalTransaction;

    private final R2dbcLargeObjectScope largeObjects =
            new R2dbcLargeObjectScope();

    private volatile BatchTransactionState state = BatchTransactionState.NEW;

    /**
     * 创建一个尚未开始事务的连接句柄。
     *
     * @param connection 当前批量操作独占的 R2DBC 连接
     */
    R2dbcBatchConnectionHandle(Connection connection) {
        this.connection = Objects.requireNonNull(connection, "batch connection must not be null");
        this.externalTransaction = null;
    }

    /** 创建外部事务连接句柄，连接、路由身份和完成通知在一次订阅内保持一致。 */
    R2dbcBatchConnectionHandle(R2dbcTransactionContext transaction) {
        this.externalTransaction = Objects.requireNonNull(transaction, "transaction context must not be null");
        this.connection = externalTransaction.connection();
    }

    /** 返回当前批量操作持有的连接。 */
    Connection connection() {
        return connection;
    }

    /** 返回连接是否由外部事务持有；外部连接不能由 ORM 提交、回滚或关闭。 */
    boolean external() {
        return externalTransaction != null;
    }

    /** 返回外部事务上下文；只允许在 {@link #external()} 为 true 时调用。 */
    R2dbcTransactionContext externalTransaction() {
        if (externalTransaction == null) {
            throw new IllegalStateException("owned batch connection does not have external transaction context");
        }
        return externalTransaction;
    }

    /** @return 当前批量连接租约内等待释放的 R2DBC 大字段句柄作用域 */
    R2dbcLargeObjectScope largeObjects() {
        return largeObjects;
    }

    /** 返回最新可见的事务状态。 */
    BatchTransactionState state() {
        return state;
    }

    /** 只有 beginTransaction 成功后才能调用，表示事务已经可以回滚。 */
    void markActive() {
        state = BatchTransactionState.ACTIVE;
    }

    /** 调用 commitTransaction 前调用，表示提交结果开始变得不可直接判断。 */
    void markCommitting() {
        state = BatchTransactionState.COMMITTING;
    }

    /** 只有 commitTransaction 成功返回后才能调用。 */
    void markCommitted() {
        state = BatchTransactionState.COMMITTED;
    }

    /** 只有 rollbackTransaction 成功返回后才能调用。 */
    void markRolledBack() {
        state = BatchTransactionState.ROLLED_BACK;
    }
}

/**
 * 批量事务的最小状态机。
 *
 * <p>NEW 表示事务尚未成功启动；ACTIVE 可以尝试回滚；COMMITTING 表示提交已经发出但结果可能未知。
 * 只有 COMMITTED 和 ROLLED_BACK 表示事务结果已经明确，可以进入正常关闭流程。</p>
 */
enum BatchTransactionState {
    NEW,
    ACTIVE,
    COMMITTING,
    COMMITTED,
    ROLLED_BACK
}
