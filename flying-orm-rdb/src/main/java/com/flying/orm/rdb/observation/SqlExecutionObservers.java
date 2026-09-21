package com.flying.orm.rdb.observation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * 常用的 SQL 观测组合工具。核心库只负责发出不含参数值的结构化事件，日志、指标和链路追踪由上层接入。
 *
 * <p>所有工厂都会把最终 observer 包装成故障隔离版本，观测代码抛出的普通运行时异常只会丢掉该次观测，
 * 不会改变数据库执行结果；直接抛出的 {@link Error} 仍原样传播。组合后的对象可以并发调用；
 * 用户提供的 observer 自身仍应保证线程安全。</p>
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public final class SqlExecutionObservers {

    private SqlExecutionObservers() {
    }

    /** 返回完全无开销行为的空 observer，适合作为默认值。 */
    public static SqlExecutionObserver noop() {
        return SqlExecutionObserver.noop();
    }

    /**
     * 隔离 observer 自身的普通运行时异常。这里只吞普通观测故障，不捕获数据库执行链中的错误；
     * 直接抛出的 {@link Error} 仍原样传播。
     *
     * @param observer 真实 observer
     * @return 隔离普通运行时故障、保留直接 {@link Error} 的包装器
     */
    public static SqlExecutionObserver safe(SqlExecutionObserver observer) {
        SqlExecutionObserver safeObserver = Objects.requireNonNull(observer,
                                                                   "sql execution observer must not be null");
        if (safeObserver instanceof Isolated) {
            return safeObserver;
        }
        try {
            if (!safeObserver.enabled()) {
                return safeObserver;
            }
        } catch (RuntimeException failure) {
            return noop();
        }
        return new SafeSqlExecutionObserver(safeObserver);
    }

    interface Isolated {
    }

    private static final class SafeSqlExecutionObserver implements SqlExecutionObserver, Isolated {
        private final SqlExecutionObserver delegate;

        private SafeSqlExecutionObserver(SqlExecutionObserver delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean enabled() {
            return true;
        }

        @Override
        public boolean requiresParameterValues() {
            try {
                return delegate.requiresParameterValues();
            } catch (RuntimeException failure) {
                // 这个探测发生在 SQL 执行前。日志组件坏了就按“不需要参数”处理，不能拦住数据库操作。
                return false;
            }
        }


        @Override
        public void onExecution(SqlExecutionObservation observation) {
            try {
                delegate.onExecution(observation);
            } catch (RuntimeException failure) {
                // 观测代码出问题时，不能反过来影响数据库执行结果。
            }
        }

        @Override
        public void onExecution(SqlExecutionObservation observation, List<Object> parameters) {
            try {
                delegate.onExecution(observation, parameters);
            } catch (RuntimeException failure) {
                // 参数脱敏或日志格式化失败时，不能影响数据库执行。
            }
        }



        @Override
        public void onResourceCleanup(ResourceCleanupObservation observation) {
            try {
                delegate.onResourceCleanup(observation);
            } catch (RuntimeException failure) {
                // 清理故障的观测出口同样必须与数据库结果隔离。
            }
        }
    }

    /** 按声明顺序调用多个 observer；单个 observer 失败不会阻止后续 observer。 */
    public static SqlExecutionObserver composite(SqlExecutionObserver... observers) {
        Objects.requireNonNull(observers, "sql execution observers must not be null");
        if (observers.length == 0) {
            return noop();
        }
        return composite(List.of(observers));
    }

    public static SqlExecutionObserver composite(Iterable<? extends SqlExecutionObserver> observers) {
        Objects.requireNonNull(observers, "sql execution observers must not be null");
        List<SqlExecutionObserver> delegates = toSafeList(observers);
        if (delegates.isEmpty()) {
            return noop();
        }
        if (delegates.size() == 1) {
            return delegates.getFirst();
        }
        // Bind each delegate's requested details once, as execution adapters do at assembly.
        boolean[] detailRequirements = new boolean[delegates.size()];
        boolean anyParameters = false;
        for (int index = 0; index < delegates.size(); index++) {
            SqlExecutionObserver delegate = delegates.get(index);
            boolean details = delegate.requiresParameterValues();
            detailRequirements[index] = details;
            anyParameters |= details;
        }
        boolean parametersRequired = anyParameters;
        return new SqlExecutionObserver() {
            @Override
            public boolean requiresParameterValues() {
                return parametersRequired;
            }


            @Override
            public void onExecution(SqlExecutionObservation observation) {
                delegates.forEach(observer -> observer.onExecution(observation));
            }

            @Override
            public void onExecution(SqlExecutionObservation observation, List<Object> parameters) {
                for (int index = 0; index < delegates.size(); index++) {
                    SqlExecutionObserver delegate = delegates.get(index);
                    if (detailRequirements[index]) {
                        delegate.onExecution(observation, parameters);
                    } else {
                        delegate.onExecution(observation);
                    }
                }
            }

            @Override
            public void onResourceCleanup(ResourceCleanupObservation observation) {
                delegates.forEach(observer -> observer.onResourceCleanup(observation));
            }
        };
    }

    /** 创建按条件过滤的 observer，适合把慢 SQL、错误和成功指标分流。 */
    public static SqlExecutionObserver when(Predicate<SqlExecutionObservation> predicate,
                                            SqlExecutionObserver observer) {
        Predicate<SqlExecutionObservation> safePredicate = Objects.requireNonNull(predicate,
                                                                                 "sql observation predicate must not be null");
        SqlExecutionObserver delegate = Objects.requireNonNull(observer,
                                                               "sql execution observer must not be null");
        return safe(new SqlExecutionObserver() {
            @Override
            public boolean enabled() {
                return delegate.enabled();
            }

            @Override
            public boolean requiresParameterValues() {
                return delegate.requiresParameterValues();
            }


            @Override
            public void onExecution(SqlExecutionObservation observation) {
                if (safePredicate.test(observation)) {
                    delegate.onExecution(observation);
                }
            }

            @Override
            public void onExecution(SqlExecutionObservation observation, List<Object> parameters) {
                if (safePredicate.test(observation)) {
                    delegate.onExecution(observation, parameters);
                }
            }



            @Override
            public void onResourceCleanup(ResourceCleanupObservation observation) {
                // 资源泄漏风险不能因为 SQL 成功/慢查询采样条件而被静默丢弃。
                delegate.onResourceCleanup(observation);
            }
        });
    }

    /** 只接收耗时达到阈值的事件，阈值为零时接收全部事件。 */
    public static SqlExecutionObserver slow(Duration threshold, SqlExecutionObserver observer) {
        Duration safeThreshold = Objects.requireNonNull(threshold, "slow sql threshold must not be null");
        if (safeThreshold.isNegative()) {
            throw new IllegalArgumentException("slow sql threshold must not be negative");
        }
        return when(observation -> Duration.ofNanos(observation.durationNanos()).compareTo(safeThreshold) >= 0,
                    observer);
    }

    public static SqlExecutionObserver errors(SqlExecutionObserver observer) {
        return when(observation -> observation.status() == SqlExecutionStatus.ERROR, observer);
    }

    public static SqlExecutionObserver successes(SqlExecutionObserver observer) {
        return when(observation -> observation.status() == SqlExecutionStatus.SUCCESS, observer);
    }

    /** 按独立随机概率采样，适合高吞吐场景降低日志和指标成本。 */
    public static SqlExecutionObserver sample(double rate, SqlExecutionObserver observer) {
        if (Double.isNaN(rate) || rate < 0D || rate > 1D) {
            throw new IllegalArgumentException("sql observation sample rate must be between 0 and 1");
        }
        SqlExecutionObserver delegate = Objects.requireNonNull(observer,
                                                               "sql execution observer must not be null");
        if (rate == 0D) {
            return noop();
        }
        if (rate == 1D) {
            return safe(delegate);
        }
        return when(ignored -> ThreadLocalRandom.current().nextDouble() < rate, delegate);
    }

    /**
     * 每固定数量事件采样一次。AtomicLong 让共享 observer 在并发请求下仍有全局稳定计数。
     */
    public static SqlExecutionObserver sampleEvery(long interval, SqlExecutionObserver observer) {
        if (interval <= 0) {
            throw new IllegalArgumentException("sql observation sample interval must be positive");
        }
        SqlExecutionObserver safeObserver = Objects.requireNonNull(observer,
                                                                   "sql execution observer must not be null");
        AtomicLong sequence = new AtomicLong();
        return when(ignored -> sequence.incrementAndGet() % interval == 0, safeObserver);
    }

    private static List<SqlExecutionObserver> toSafeList(Iterable<? extends SqlExecutionObserver> observers) {
        List<SqlExecutionObserver> delegates = new ArrayList<>();
        for (SqlExecutionObserver observer : observers) {
            SqlExecutionObserver safeObserver = safe(Objects.requireNonNull(
                    observer, "sql execution observer must not be null"));
            if (safeObserver.enabled()) {
                delegates.add(safeObserver);
            }
        }
        return List.copyOf(delegates);
    }
}
