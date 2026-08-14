package com.flying.orm.rdb.isolation;

import io.r2dbc.spi.Connection;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * 在连接借出后设置 schema/RLS，在连接归还池前清理会话。
 *
 * <p>实现必须非阻塞，并且 reset 要能重复调用。初始化或 reset 失败时，路由工厂会调用配置的连接失效器；
 * 没有配置可证明物理淘汰的能力时会明确 fail-closed，生产连接池必须提供该适配器，不能把普通 close
 * 冒充物理淘汰。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public interface R2dbcSessionCustomizer {

    Publisher<Void> initialize(Connection connection, IsolationContext context);

    Publisher<Void> reset(Connection connection, IsolationContext context);

    static R2dbcSessionCustomizer none() {
        return NoopR2dbcSessionCustomizer.INSTANCE;
    }
}

/** 空实现只是默认装配细节，不进入调用方的公共类型体系。 */
final class NoopR2dbcSessionCustomizer implements R2dbcSessionCustomizer {

    static final NoopR2dbcSessionCustomizer INSTANCE = new NoopR2dbcSessionCustomizer();

    private NoopR2dbcSessionCustomizer() {
    }

    @Override
    public Publisher<Void> initialize(Connection connection, IsolationContext context) {
        return Mono.empty();
    }

    @Override
    public Publisher<Void> reset(Connection connection, IsolationContext context) {
        return Mono.empty();
    }
}
