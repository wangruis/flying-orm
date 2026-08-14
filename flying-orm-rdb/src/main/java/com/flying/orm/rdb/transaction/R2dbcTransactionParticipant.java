package com.flying.orm.rdb.transaction;

import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.util.Objects;

/**
 * 将任意 Java 生态的外部响应式事务接入 flying-orm 的统一入口。
 *
 * <p>没有事务时返回空 {@link Mono}；有事务时返回已绑定的 {@link R2dbcTransactionContext}。接口不依赖
 * 应用框架，也不假设具体连接池。使用 Reactor Context 的框架可以直接使用 {@link #reactorContext()}，
 * 其他框架只需实现一次 {@link #currentTransaction()} 适配自己的事务上下文。</p>
 * @author wangr
 * @version v1.0
 */
@FunctionalInterface
public interface R2dbcTransactionParticipant {

    /**
     * 在订阅时取得当前外部事务。返回空表示本次操作由 flying-orm 按原有方式独立取连接并管理资源。
     */
    Mono<R2dbcTransactionContext> currentTransaction();

    /**
     * 取得当前事务并核对本次调用要求的数据库路由。路由键为空表示调用方没有提出切库要求；有值时必须与事务
     * 开始时锁定的 {@link R2dbcTransactionContext#routingIdentity()} 完全一致，否则在执行 SQL 前拒绝。
     *
     * <p>默认方法让现有框架适配器仍然只实现 {@link #currentTransaction()}。路由安全由 ORM 统一执行，
     * 不要求每个上层容器适配器重复写比较逻辑。</p>
     *
     * @param requestedRoutingIdentity 当前调用明确选择的数据库路由键；没有显式路由时为 {@code null}
     * @return 不存在外部事务时为空，存在时返回已验证路由的事务上下文
     */
    default Mono<R2dbcTransactionContext> currentTransaction(String requestedRoutingIdentity) {
        String requested = requestedRoutingIdentity == null ? null : requestedRoutingIdentity.trim();
        if (requested != null && requested.isEmpty()) {
            return Mono.error(new IllegalArgumentException("requested routing identity must not be blank"));
        }
        return Mono.defer(this::currentTransaction).map(transaction -> {
            R2dbcTransactionContext safeTransaction = Objects.requireNonNull(
                    transaction, "transaction context must not be null");
            if (requested != null && !requested.equals(safeTransaction.routingIdentity())) {
                throw new R2dbcTransactionParticipationException(
                        R2dbcTransactionParticipationException.Reason.ROUTING_IDENTITY_CHANGED);
            }
            return safeTransaction;
        });
    }

    /** 返回永远没有外部事务的默认参与者，保留 V1.0.0 的独立执行行为。 */
    static R2dbcTransactionParticipant none() {
        return Mono::empty;
    }

    /**
     * 从 Reactor Context 读取事务上下文。上层在事务开始后调用 {@link #bind(Context, R2dbcTransactionContext)}
     * 即可把同一连接和固定路由身份传入后续 flying-orm 操作。
     */
    static R2dbcTransactionParticipant reactorContext() {
        return () -> Mono.deferContextual(context -> Mono.justOrEmpty(context.getOrEmpty(R2dbcTransactionContext.class)));
    }

    /**
     * 把外部事务绑定到已有 Reactor Context。这里不创建连接，也不启动事务，避免 ORM 反向介入上层生命周期。
     */
    static Context bind(Context context, R2dbcTransactionContext transaction) {
        return Objects.requireNonNull(context, "reactor context must not be null")
                      .put(R2dbcTransactionContext.class,
                           Objects.requireNonNull(transaction, "transaction context must not be null"));
    }
}
