package com.flying.orm.rdb.transaction;

import java.sql.Connection;
import java.util.Objects;

/**
 * 上层事务已经开始后，交给 flying-orm 使用的 JDBC 连接快照。
 *
 * <p>这个对象只说明“当前事务正在使用哪条连接”，它不是事务管理器。
 * flying-orm 可以在这条连接上创建并关闭 Statement、ResultSet，但不能提交、回滚或关闭 Connection。
 * 最终事务结果和连接归还始终由提供该上下文的上层系统负责。</p>
 *
 * @param connection      上层事务已经绑定的 JDBC 连接
 * @param completion      上层事务结束通知注册器
 * @author wangr
 * @version v2.0.0
 */
public record JdbcTransactionContext(Connection connection,
                                     JdbcTransactionCompletion completion) {

    /** 兼容只提供事务连接的适配器；SQL 可以参与事务，但没有最终完成通知。 */
    public JdbcTransactionContext(Connection connection) {
        this(connection, JdbcTransactionCompletion.unavailable());
    }

    public JdbcTransactionContext {
        connection = Objects.requireNonNull(connection, "transaction connection must not be null");
        completion = Objects.requireNonNull(completion, "transaction completion must not be null");
    }

    /** 用上层持有的连接创建事务上下文。 */
    public static JdbcTransactionContext external(Connection connection) {
        return new JdbcTransactionContext(connection, JdbcTransactionCompletion.unavailable());
    }

    /** 用外部连接和真正的事务结束通知创建上下文，完整事务适配器应优先使用这个入口。 */
    public static JdbcTransactionContext external(Connection connection,
                                                  JdbcTransactionCompletion completion) {
        return new JdbcTransactionContext(connection,
                                          Objects.requireNonNull(completion, "transaction completion must not be null"));
    }
}
