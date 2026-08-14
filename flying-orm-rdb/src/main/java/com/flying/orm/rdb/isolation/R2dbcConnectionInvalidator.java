package com.flying.orm.rdb.isolation;

import io.r2dbc.spi.Connection;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Function;

/**
 * 区分可复用连接归还与异常连接物理淘汰的 R2DBC 资源边界。
 *
 * <p>{@link #close(Connection)} 只用于会话状态已经确认干净的普通关闭；{@link #invalidate(Connection)}
 * 用于初始化、reset、rollback 或普通关闭失败后状态不可证明安全的连接。默认实现对失效采用 fail-closed：
 * 没有显式物理淘汰能力时返回错误，绝不把普通 {@code close()} 冒充物理淘汰。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public interface R2dbcConnectionInvalidator {

    /**
     * 关闭或归还已确认可复用的连接。
     *
     * @param connection 已完成业务且会话状态安全的连接
     * @return 关闭或归还完成信号
     */
    Publisher<Void> close(Connection connection);

    /**
     * 物理淘汰状态不可证明安全的连接，不得退化为普通可复用关闭。
     *
     * @param connection 必须从可复用池中淘汰的异常连接
     * @return 物理淘汰完成信号；无法证明淘汰时必须报错
     */
    Publisher<Void> invalidate(Connection connection);

    /**
     * 创建默认 fail-closed 实现。普通关闭仍调用 R2DBC {@code close()}，异常失效因为没有物理连接
     * 适配信息而明确失败。
     *
     * @return 默认安全实现
     */
    static R2dbcConnectionInvalidator failClosed() {
        return FailClosedInvalidator.INSTANCE;
    }

    /**
     * 用部署环境已知且可证明的连接池能力创建适配器。R2DBC SPI 没有定义池引用淘汰 API，单纯解包并调用
     * 物理连接 {@code close()} 不能证明连接已从池中立即驱逐，因此框架不提供基于类型或 {@code Wrapped}
     * 猜测的伪安全实现。连接池集成必须把其公开的普通归还与物理淘汰入口分别传入本工厂。
     * 两个函数必须返回冷或可重复安全订阅的 Publisher，且失效函数必须真正从池中淘汰连接。
     *
     * @param close      普通可复用关闭函数
     * @param invalidate 异常连接物理淘汰函数
     * @return 显式连接失效适配器
     */
    static R2dbcConnectionInvalidator of(
            Function<? super Connection, ? extends Publisher<Void>> close,
            Function<? super Connection, ? extends Publisher<Void>> invalidate) {
        Function<? super Connection, ? extends Publisher<Void>> safeClose =
                Objects.requireNonNull(close, "normal connection close function must not be null");
        Function<? super Connection, ? extends Publisher<Void>> safeInvalidate =
                Objects.requireNonNull(invalidate, "physical connection invalidation function must not be null");
        return new R2dbcConnectionInvalidator() {
            @Override
            public Publisher<Void> close(Connection connection) {
                Connection safeConnection = Objects.requireNonNull(connection, "connection must not be null");
                return Mono.defer(() -> Mono.from(Objects.requireNonNull(
                        safeClose.apply(safeConnection), "normal connection close publisher must not be null")));
            }

            @Override
            public Publisher<Void> invalidate(Connection connection) {
                Connection safeConnection = Objects.requireNonNull(connection, "connection must not be null");
                return Mono.defer(() -> Mono.from(Objects.requireNonNull(
                        safeInvalidate.apply(safeConnection),
                        "physical connection invalidation publisher must not be null")));
            }
        };
    }
}

/**
 * 默认失效路径无法证明物理淘汰时保留错误，避免污染连接重新进入池中。
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
enum FailClosedInvalidator implements R2dbcConnectionInvalidator {
    INSTANCE;

    @Override
    public Publisher<Void> close(Connection connection) {
        Connection safeConnection = Objects.requireNonNull(connection, "connection must not be null");
        return Mono.defer(() -> Mono.from(safeConnection.close()));
    }

    @Override
    public Publisher<Void> invalidate(Connection connection) {
        Objects.requireNonNull(connection, "connection must not be null");
        return Mono.error(new IllegalStateException("physical connection invalidation is not configured"));
    }
}
