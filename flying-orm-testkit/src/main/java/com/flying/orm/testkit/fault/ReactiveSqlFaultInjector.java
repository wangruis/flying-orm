package com.flying.orm.testkit.fault;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * 给响应式 SQL 执行器套一层可重复的故障脚本。
 * 规则按操作类型和订阅序号匹配，因此并发测试不会依赖碰运气的 sleep 或真实断网。
 * 这个类只属于 testkit，不应该放进生产服务的执行链。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class ReactiveSqlFaultInjector implements ReactiveSqlExecutor {

    private final ReactiveSqlExecutor delegate;
    private final Map<RuleKey, Fault> rules;
    private final Map<Operation, AtomicLong> invocations;
    private final Map<Operation, LongAdder> cancellations;

    private ReactiveSqlFaultInjector(ReactiveSqlExecutor delegate, Map<RuleKey, Fault> rules) {
        this.delegate = Objects.requireNonNull(delegate, "fault injector delegate must not be null");
        this.rules = Map.copyOf(rules);
        this.invocations = counters(AtomicLong::new);
        this.cancellations = counters(LongAdder::new);
    }

    /** 从一个真实或内存执行器开始编排故障。 */
    public static Builder builder(ReactiveSqlExecutor delegate) {
        return new Builder(delegate);
    }

    /** 返回某类操作已经订阅了多少次。调用序号从 1 开始。 */
    public long invocations(Operation operation) {
        return invocations.get(requireOperation(operation)).get();
    }

    /** 返回某类操作被下游取消了多少次。 */
    public long cancellations(Operation operation) {
        return cancellations.get(requireOperation(operation)).sum();
    }

    @Override
    public Flux<DynamicRow> query(SqlRequest request) {
        Objects.requireNonNull(request, "sql request must not be null");
        return applyFlux(Operation.QUERY, () -> delegate.query(request));
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request) {
        Objects.requireNonNull(request, "sql request must not be null");
        return applyMono(Operation.UPDATE, () -> delegate.rowsUpdated(request), Long.class);
    }

    @Override
    public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
        Objects.requireNonNull(request, "batch write request must not be null");
        return applyMono(Operation.WRITE_BATCH, () -> delegate.writeBatch(request), BatchWriteResult.class);
    }

    @Override
    public Flux<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
        Objects.requireNonNull(request, "batch write request must not be null");
        return applyFlux(Operation.WRITE_BATCH_CHUNKS, () -> delegate.writeBatchChunks(request));
    }

    @Override
    public Mono<BatchResolution> resolveUnknown(BatchChunkResult.RecoveryToken token) {
        Objects.requireNonNull(token, "batch recovery token must not be null");
        return applyMono(Operation.RESOLVE_UNKNOWN,
                         () -> delegate.resolveUnknown(token),
                         BatchResolution.class);
    }

    private <T> Flux<T> applyFlux(Operation operation, Supplier<Flux<T>> source) {
        return Flux.defer(() -> {
            Fault fault = nextFault(operation);
            Flux<T> result = switch (fault) {
                case null -> safeFlux(source);
                case FailureFault failure -> Flux.error(failure.error());
                case DelayFault delay -> Mono.delay(delay.duration())
                                             .thenMany(Flux.defer(() -> safeFlux(source)));
                case HangFault ignored -> Flux.never();
                case ValueFault value -> Flux.error(invalidValueFault(operation, value.value()));
            };
            return result.doOnCancel(() -> cancellations.get(operation).increment());
        });
    }

    private <T> Mono<T> applyMono(Operation operation, Supplier<Mono<T>> source, Class<T> resultType) {
        return Mono.defer(() -> {
            Fault fault = nextFault(operation);
            Mono<T> result = switch (fault) {
                case null -> safeMono(source);
                case FailureFault failure -> Mono.error(failure.error());
                case DelayFault delay -> Mono.delay(delay.duration())
                                             .then(Mono.defer(() -> safeMono(source)));
                case HangFault ignored -> Mono.never();
                case ValueFault value -> resultType.isInstance(value.value())
                        ? Mono.just(resultType.cast(value.value()))
                        : Mono.error(invalidValueFault(operation, value.value()));
            };
            return result.doOnCancel(() -> cancellations.get(operation).increment());
        });
    }

    private Fault nextFault(Operation operation) {
        long invocation = invocations.get(operation).incrementAndGet();
        return rules.get(new RuleKey(operation, invocation));
    }

    private static <T> Flux<T> safeFlux(Supplier<Flux<T>> source) {
        return Objects.requireNonNull(source.get(), "fault injector delegate returned null Flux");
    }

    private static <T> Mono<T> safeMono(Supplier<Mono<T>> source) {
        return Objects.requireNonNull(source.get(), "fault injector delegate returned null Mono");
    }

    private static IllegalStateException invalidValueFault(Operation operation, Object value) {
        return new IllegalStateException("fault value " + value.getClass().getName()
                                                 + " cannot be returned by " + operation);
    }

    private static Operation requireOperation(Operation operation) {
        return Objects.requireNonNull(operation, "fault operation must not be null");
    }

    private static void requireInvocation(long invocation) {
        if (invocation <= 0) {
            throw new IllegalArgumentException("fault invocation must be greater than zero");
        }
    }

    private static <T> Map<Operation, T> counters(Supplier<T> factory) {
        Map<Operation, T> counters = new EnumMap<>(Operation.class);
        for (Operation operation : Operation.values()) {
            counters.put(operation, factory.get());
        }
        return counters;
    }

    /** 可以独立计数和注入故障的执行入口。 */
    public enum Operation {
        QUERY,
        UPDATE,
        WRITE_BATCH,
        WRITE_BATCH_CHUNKS,
        RESOLVE_UNKNOWN
    }

    /**
     * 规则构建器只在组装阶段使用，build 后规则会复制成不可变 Map。
     */
    public static final class Builder {

        private final ReactiveSqlExecutor delegate;
        private final Map<RuleKey, Fault> rules = new HashMap<>();

        private Builder(ReactiveSqlExecutor delegate) {
            this.delegate = Objects.requireNonNull(delegate, "fault injector delegate must not be null");
        }

        /** 指定某次订阅直接失败，delegate 不会被调用。 */
        public Builder fail(Operation operation, long invocation, Throwable error) {
            return add(operation,
                       invocation,
                       new FailureFault(Objects.requireNonNull(error, "injected error must not be null")));
        }

        /** 指定某次订阅延迟后再调用 delegate。 */
        public Builder delay(Operation operation, long invocation, Duration duration) {
            Duration safeDuration = Objects.requireNonNull(duration, "injected delay must not be null");
            if (safeDuration.isNegative()) {
                throw new IllegalArgumentException("injected delay must not be negative");
            }
            return add(operation, invocation, new DelayFault(safeDuration));
        }

        /** 指定某次订阅一直不结束，调用方可用 timeout 或 cancel 验证资源释放。 */
        public Builder hang(Operation operation, long invocation) {
            return add(operation, invocation, HangFault.INSTANCE);
        }

        /** 直接返回批量结果，主要用于稳定复现 UNKNOWN 及部分成功。 */
        public Builder returnBatch(long invocation, BatchWriteResult result) {
            return add(Operation.WRITE_BATCH,
                       invocation,
                       new ValueFault(Objects.requireNonNull(result, "injected batch result must not be null")));
        }

        /** 直接返回 UNKNOWN 查询结果，不访问 delegate。 */
        public Builder returnRecovery(long invocation, BatchResolution resolution) {
            return add(Operation.RESOLVE_UNKNOWN,
                       invocation,
                       new ValueFault(Objects.requireNonNull(resolution,
                                                             "injected batch resolution must not be null")));
        }

        /** 固化规则并创建线程安全的故障执行器。 */
        public ReactiveSqlFaultInjector build() {
            return new ReactiveSqlFaultInjector(delegate, rules);
        }

        private Builder add(Operation operation, long invocation, Fault fault) {
            Operation safeOperation = requireOperation(operation);
            requireInvocation(invocation);
            RuleKey key = new RuleKey(safeOperation, invocation);
            if (rules.putIfAbsent(key, fault) != null) {
                throw new IllegalArgumentException("fault rule already exists for " + safeOperation
                                                           + " invocation " + invocation);
            }
            return this;
        }
    }

    private record RuleKey(Operation operation, long invocation) {
    }

    private sealed interface Fault permits FailureFault, DelayFault, HangFault, ValueFault {
    }

    private record FailureFault(Throwable error) implements Fault {
    }

    private record DelayFault(Duration duration) implements Fault {
    }

    private enum HangFault implements Fault {
        INSTANCE
    }

    private record ValueFault(Object value) implements Fault {
    }
}
