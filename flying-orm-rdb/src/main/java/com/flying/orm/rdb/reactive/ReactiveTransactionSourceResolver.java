package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * 在订阅时把当前事务上下文收口成日志和指标使用的三种稳定来源。
 *
 * <p>查询动作必须保持惰性，不能在客户端创建阶段读取某个请求的事务。查不到外部事务时由调用方决定本次操作
 * 是自动提交还是 ORM 自管事务。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class ReactiveTransactionSourceResolver {

    private final Supplier<Mono<R2dbcTransactionContext>> currentTransaction;

    ReactiveTransactionSourceResolver(Supplier<Mono<R2dbcTransactionContext>> currentTransaction) {
        this.currentTransaction = Objects.requireNonNull(currentTransaction,
                                                         "current transaction supplier must not be null");
    }

    Mono<Resolution> resolve(SqlTransactionSource localSource) {
        SqlTransactionSource safeLocalSource = Objects.requireNonNull(localSource,
                                                                      "local transaction source must not be null");
        return Mono.defer(() -> Objects.requireNonNull(currentTransaction.get(),
                                                       "current transaction publisher must not be null"))
                   .map(transaction -> new Resolution(SqlTransactionSource.EXTERNAL, transaction))
                   .defaultIfEmpty(new Resolution(safeLocalSource, null));
    }

    /**
     * 一次事务查找的结果会放进当前订阅上下文，后面的连接获取不能再向上层查第二次。
     * {@code transaction} 为空明确表示本次没有外部事务，不是“尚未检查”。
     */
    record Resolution(SqlTransactionSource source, R2dbcTransactionContext transaction) {

        Resolution {
            source = Objects.requireNonNull(source, "resolved transaction source must not be null");
            if ((source == SqlTransactionSource.EXTERNAL) != (transaction != null)) {
                throw new IllegalArgumentException("external transaction source and context must appear together");
            }
        }

        <T> Flux<T> bind(Flux<T> publisher) {
            return Objects.requireNonNull(publisher, "transaction source flux must not be null")
                          .contextWrite(context -> context.put(Resolution.class, this));
        }

        <T> Mono<T> bind(Mono<T> publisher) {
            return Objects.requireNonNull(publisher, "transaction source mono must not be null")
                          .contextWrite(context -> context.put(Resolution.class, this));
        }
    }
}
