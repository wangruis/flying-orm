package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 优先复用当前订阅已经解析过的事务结果，避免 observer、执行会话和批量写入重复询问上层事务管理器。
 *
 * <p>上下文里同时缓存“存在外部事务”和“明确没有外部事务”两种结果。缓存只活在一次订阅中，
 * 不会跨请求共享，也不会把事务连接保存进执行器单例。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class ResolvedTransactionParticipant implements R2dbcTransactionParticipant {

    private final R2dbcTransactionParticipant delegate;

    private ResolvedTransactionParticipant(R2dbcTransactionParticipant delegate) {
        this.delegate = Objects.requireNonNull(delegate, "transaction participant must not be null");
    }

    static R2dbcTransactionParticipant wrap(R2dbcTransactionParticipant participant) {
        if (participant instanceof ResolvedTransactionParticipant) {
            return participant;
        }
        return new ResolvedTransactionParticipant(participant);
    }

    @Override
    public Mono<R2dbcTransactionContext> currentTransaction() {
        return Mono.deferContextual(context -> {
            ReactiveTransactionSourceResolver.Resolution resolution = context.getOrDefault(
                    ReactiveTransactionSourceResolver.Resolution.class, null);
            if (resolution != null) {
                return Mono.justOrEmpty(resolution.transaction());
            }
            // Repository 用接口类型作为仅限本次订阅的内部 key，none() 表示事务 absence 已经解析过。
            R2dbcTransactionParticipant resolved = context.getOrDefault(
                    R2dbcTransactionParticipant.class, null);
            if (resolved != null) {
                return Objects.requireNonNull(
                        resolved.currentTransaction(), "resolved transaction publisher must not be null");
            }
            return context.<R2dbcTransactionContext>getOrEmpty(R2dbcTransactionContext.class)
                    .map(Mono::just)
                    .orElseGet(delegate::currentTransaction);
        });
    }
}
