package com.flying.orm.benchmark;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.SqlExecutionLogObserver;
import com.flying.orm.rdb.observation.SqlExecutionLogOptions;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 数据库延迟降到接近 0 时，测执行保护和观测包装本身增加了多少调用开销。
 * 这里不冒充真实数据库吞吐；真实库结果仍由 testkit 场景和外部压测提供。
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ReactiveExecutorOverheadBenchmark {

    private SqlRequest request;

    private ReactiveSqlExecutor direct;

    private ReactiveSqlExecutor protectedExecutor;

    private ReactiveSqlExecutor observedExecutor;

    private ReactiveSqlExecutor structuredLogExecutor;

    /**
     * 使用立即完成的 Publisher，把数据库网络和驱动耗时从这条基准里排除。
     */
    @Setup(Level.Trial)
    public void setUp() {
        request = new SqlRequest("update benchmark_users set enabled = ? where id = ?",
                                 List.of(true, "u1"));
        direct = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.just(DynamicRow.copyOf(java.util.Map.of("id", "u1")));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(1L);
            }
        };
        protectedExecutor = direct.withDefaultExecutionOptions(SqlExecutionOptions.unlimited());
        observedExecutor = direct.withObserver(ignored -> {
            // 空 observer 仍会完整创建观测对象，只是不把结果交给外部指标系统。
        });
        SqlExecutionLogObserver logObserver = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(),
                ignored -> {
                    // 只测结构化日志格式化成本，不把控制台或磁盘 I/O 混进框架开销。
                });
        structuredLogExecutor = direct.withObserver(logObserver);
    }

    /**
     * @return 日志关闭时不加观测包装的响应式更新结果；这是默认装配采用的快路径
     */
    @Benchmark
    public Long directUpdate() {
        return direct.rowsUpdated(request).block();
    }

    /**
     * @return 经过默认执行保护包装后的更新结果
     */
    @Benchmark
    public Long protectedUpdate() {
        return protectedExecutor.rowsUpdated(request).block();
    }

    /**
     * @return 完整生成一次执行观测后的更新结果
     */
    @Benchmark
    public Long observedUpdate() {
        return observedExecutor.rowsUpdated(request).block();
    }

    /**
     * @return 开启结构化日志但不展示 SQL 和参数时的更新结果
     */
    @Benchmark
    public Long structuredLogUpdate() {
        return structuredLogExecutor.rowsUpdated(request).block();
    }
}
