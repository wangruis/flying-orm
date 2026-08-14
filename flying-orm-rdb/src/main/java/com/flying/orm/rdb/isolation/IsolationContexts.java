package com.flying.orm.rdb.isolation;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.Objects;

/** 把隔离信息放进 Reactor Context，保证异步切线程后仍能跟随当前订阅。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class IsolationContexts {

    private static final Object KEY = IsolationContext.class;

    private IsolationContexts() {
    }

    public static <T> Mono<T> with(Mono<T> source, IsolationContext context) {
        return Objects.requireNonNull(source, "source must not be null")
                      .contextWrite(current -> current.put(KEY, Objects.requireNonNull(
                              context, "isolation context must not be null")));
    }

    public static <T> Flux<T> with(Flux<T> source, IsolationContext context) {
        return Objects.requireNonNull(source, "source must not be null")
                      .contextWrite(current -> current.put(KEY, Objects.requireNonNull(
                              context, "isolation context must not be null")));
    }

    /**
     * 返回当前订阅明确选择的数据库路由键。没有切库要求时返回 {@code null}，调用方应继续使用默认数据源。
     * 事务参与者用这个值检查事务开始后有没有被要求切换到另一个物理数据库。
     */
    public static String currentDatabaseKey(ContextView contextView) {
        return current(Objects.requireNonNull(contextView, "reactor context must not be null")).databaseKey();
    }

    static IsolationContext current(ContextView contextView) {
        return contextView.getOrDefault(KEY, IsolationContext.shared());
    }
}
