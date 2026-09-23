package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * 上层实现的 JDBC 连接获取与释放端口。
 *
 * <p>实现决定连接来源和释放策略；ORM 不探测事务、不创建连接、不直接关闭外部连接。
 * 一个同连接工作单元成功取得连接后，ORM 在释放自己创建的语句和结果集后调用一次释放端口。
 * 获取失败不调用释放；释放失败按错误事实传播，不推断已执行 SQL 的提交状态。</p>
 *
 * <p>同一工作单元向两个方法传递同一个代表请求。普通 SQL 使用当前请求，多语句工作使用首条实际请求；
 * 仅没有 SQL 的 JDBC 元数据回调使用 {@code null}，不伪造 SQL 或参数。</p>
 *
 * @author wangr
 * @version v4.1.0
 */
public interface JdbcConnectionAccess {

    /** 用上层获取/释放函数接入；不保存 DataSource，也不决定连接生命周期。 */
    static JdbcConnectionAccess of(Acquirer acquire, Releaser release) {
        Objects.requireNonNull(acquire, "connection acquirer must not be null");
        Objects.requireNonNull(release, "connection releaser must not be null");
        return new JdbcConnectionAccess() {
            @Override
            public Connection getConnection(SqlRequest request) throws SQLException {
                return acquire.getConnection(request);
            }
            @Override
            public void releaseConnection(Connection connection, SqlRequest request) throws SQLException {
                release.releaseConnection(connection, request);
            }
        };
    }

    /** 允许直接传播 SQLException 的上层获取函数。 */
    @FunctionalInterface
    interface Acquirer {
        Connection getConnection(SqlRequest request) throws SQLException;
    }

    /** 允许直接传播 SQLException 的上层释放函数。 */
    @FunctionalInterface
    interface Releaser {
        void releaseConnection(Connection connection, SqlRequest request) throws SQLException;
    }

    /**
     * 取得本次工作使用的连接。
     *
     * @param request 代表 SQL 请求；无 SQL 的元数据回调为 null
     * @return 非 null 的上层连接
     * @throws SQLException 获取失败
     */
    Connection getConnection(SqlRequest request) throws SQLException;

    /**
     * 按上层策略释放本次连接使用权；固定连接的实现可以为空操作。
     *
     * @param connection 本端口为该工作单元提供的连接
     * @param request 获取时使用的同一请求
     * @throws SQLException 释放失败
     */
    void releaseConnection(Connection connection, SqlRequest request) throws SQLException;
}
