package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import io.r2dbc.spi.Connection;
import org.reactivestreams.Publisher;
import reactor.core.publisher.SignalType;

import java.util.Objects;
import java.util.function.Function;

/**
 * 上层实现的非阻塞 R2DBC 连接获取与释放端口。
 *
 * <p>连接来源和释放策略属于实现；ORM 不探测事务、不创建连接、不直接关闭外部连接。
 * 获取和释放都在订阅链内组合，不允许内部调用 {@code block()} 或自行 {@code subscribe()}。
 * 每次订阅取得连接后，工作完成、失败或取消均调用一次释放，获取失败不虚构释放。</p>
 *
 * <p>同一工作单元向两个方法传递同一个代表请求：普通 SQL 使用当前请求，多语句工作使用首条实际请求。
 * ORM 先处理自己拥有的结果和 LOB 资源，再订阅释放 Publisher；释放错误不能作为成功归还处理。</p>
 *
 * @author wangr
 * @version v4.1.0
 */
public interface R2dbcConnectionAccess {

    /** 用上层非阻塞函数接入；获取和释放调用仍由执行器在订阅链内组合。 */
    static R2dbcConnectionAccess of(Function<SqlRequest, Publisher<? extends Connection>> acquire, Releaser release) {
        Objects.requireNonNull(acquire, "connection acquirer must not be null");
        Objects.requireNonNull(release, "connection releaser must not be null");
        return new R2dbcConnectionAccess() {
            @Override
            public Publisher<? extends Connection> getConnection(SqlRequest request) {
                return acquire.apply(request);
            }
            @Override
            public Publisher<Void> releaseConnection(SignalType signal, Connection connection, SqlRequest request) {
                return release.releaseConnection(signal, connection, request);
            }
        };
    }

    /** 上层释放函数接收完成、失败或取消信号；该信号不代表事务状态。 */
    @FunctionalInterface
    interface Releaser {
        Publisher<Void> releaseConnection(SignalType signal, Connection connection, SqlRequest request);
    }

    /**
     * 返回冷 Publisher；每次订阅恰好提供一条连接，获取失败发出错误，不以空完成代替连接。
     *
     * @param request 工作单元的代表 SQL 请求
     * @return 非 null 的连接 Publisher
     */
    Publisher<? extends Connection> getConnection(SqlRequest request);

    /**
     * 返回冷 Publisher，完成表示上层释放策略已执行完毕；固定连接可返回空完成。
     *
     * @param signal 工作的 ON_COMPLETE、ON_ERROR 或 CANCEL 信号，不表示事务终态
     * @param connection 本端口为该工作单元提供的连接
     * @param request 获取时使用的同一请求
     * @return 非 null 的释放 Publisher
     */
    Publisher<Void> releaseConnection(SignalType signal, Connection connection, SqlRequest request);
}
