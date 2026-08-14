package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * 给多行结果流加绝对截止时间。普通 Flux timeout 会在每行到达后重新计时，只能发现“多久没有下一行”；
 * 这里的计时器从订阅开始只启动一次，限制的是拿到并消费完整结果的总时长。
 *
 * <p>截止时间到达时 takeUntilOther 会取消上游，连接由外层 usingWhen 释放。驱动是否能把取消继续传给数据库
 * 由具体 R2DBC 实现决定，因此超时代表客户端停止等待，不承诺数据库端语句已经立刻终止。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
final class SqlExecutionTimeouts {

    private SqlExecutionTimeouts() {
    }

    static <T> Flux<T> total(Flux<T> source, Duration timeout) {
        Flux<T> safeSource = Objects.requireNonNull(source, "sql result source must not be null");
        Duration safeTimeout = Objects.requireNonNull(timeout, "sql execution timeout must not be null");
        return Flux.defer(() -> {
            /*
             * timeout(first, next) 原本会在每行到达后换一个新计时器。这里把同一个 Mono.cache() 同时交给
             * 首行和后续行：第一次监听时计时器开始运行，后面的监听只能接着等这一个截止点，不能重新计时。
             * cache 还有一个关键作用：timeout 在切换监听时会取消旧订阅，但不会连带停掉已经启动的截止计时器。
             * 截止后 timeout 会取消结果流上游，再把标准 TimeoutException 转成项目统一的执行超时异常。
             */
            Mono<Long> deadline = Mono.delay(safeTimeout).cache();
            return safeSource.timeout(deadline, ignored -> deadline)
                             .onErrorMap(TimeoutException.class,
                                         error -> new SqlExecutionTimeoutException(safeTimeout, error));
        });
    }
}
