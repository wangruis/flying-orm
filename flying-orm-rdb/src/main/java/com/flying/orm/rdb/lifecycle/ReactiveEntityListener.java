package com.flying.orm.rdb.lifecycle;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * 不依赖 Spring 的实体生命周期扩展点。
 *
 * <p>监听器可能被多个 R2DBC 订阅同时调用，实现类必须并发安全。返回值就是执行链的一部分，可以异步完成，
 * 也可以返回 {@link Mono#empty()}；不要在回调里调用 block、JDBC 或其他阻塞 API。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
@FunctionalInterface
public interface ReactiveEntityListener<T> {

    /**
     * @return 本阶段完成信号；PRE 阶段失败会阻止数据库操作，POST 写阶段失败会以
     * {@link CommittedEntityLifecycleException} 明确报告“数据库已提交”，避免调用方盲目重试
     */
    Publisher<Void> onEvent(EntityLifecycleEvent<T> event);

    /** @return 不产生额外工作、可全局复用的监听器 */
    static <T> ReactiveEntityListener<T> none() {
        return event -> Mono.empty();
    }

    /**
     * 按声明顺序串行组合监听器。这里故意不用并发 merge，审计、校验和字段填充才能得到可预测的先后关系。
     */
    @SafeVarargs
    static <T> ReactiveEntityListener<T> compose(ReactiveEntityListener<T>... listeners) {
        List<ReactiveEntityListener<T>> ordered = List.of(listeners).stream()
                                                       .map(listener -> Objects.requireNonNull(
                                                               listener, "entity lifecycle listener must not be null"))
                                                       .toList();
        return event -> reactor.core.publisher.Flux.fromIterable(ordered)
                                                    .concatMap(listener -> Mono.from(listener.onEvent(event)))
                                                    .then();
    }
}
