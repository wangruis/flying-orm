package com.flying.orm.benchmark.database;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runner 单元测试只校验公开参数，普通构建不会尝试连接数据库。 */
class RealDatabasePerformanceRunnerTest {

    @Test
    void parsesFixedPerformanceParametersWithoutOpeningDatabase() {
        ReactivePerformanceArguments arguments = ReactivePerformanceArguments.parse(new String[]{
                "--mysql-url", "r2dbc:mysql://local/test",
                "--mysql-diagnostics-url", "r2dbc:mysql://monitor/local",
                "--output", "target/metrics.json",
                "--summary", "target/summary.md",
                "--git-commit", "abcdef1",
                "--warmup-seconds", "1",
                "--measurement-seconds", "2",
                "--pool-size", "8",
                "--query-concurrency", "8",
                "--batch-concurrency", "2",
                "--batch-size", "16",
                "--independent-chunk-size", "4",
                "--independent-concurrency", "4",
                "--seed-rows", "1000",
                "--phase-diagnostics", "true"
        });

        DatabasePerformanceReport.Parameters parameters = arguments.reportParameters();
        assertEquals(1, parameters.warmupSeconds());
        assertEquals(2, parameters.measurementSeconds());
        assertEquals(8, parameters.poolSize());
        assertEquals(16, parameters.batchSize());
        assertEquals(true, arguments.mysqlDiagnosticsConfigured());
        assertEquals(true, arguments.phaseDiagnostics());
    }

    /** 验证单变量诊断参数会进入真实查询执行选项，避免只记录参数却仍使用固定抓取大小。 */
    @Test
    void appliesExplicitFetchSizeToReactiveQueryOptions() {
        ReactivePerformanceArguments arguments = ReactivePerformanceArguments.parse(new String[]{
                "--postgresql-url", "r2dbc:postgresql://local/test",
                "--output", "target/metrics.json",
                "--summary", "target/summary.md",
                "--git-commit", "abcdef1",
                "--fetch-size", "256"
        });

        assertEquals(256, ReactivePerformanceDatabaseRunner.queryOptions(arguments).fetchSize());
        assertEquals(256, arguments.reportParameters().fetchSizeOverride());
        assertEquals(256, arguments.reportParameters().effectiveFetchSize());
    }

    /** 未声明诊断覆盖值时应测量项目真实默认值，不能由 benchmark 悄悄改写执行策略。 */
    @Test
    void keepsProductionFetchDefaultWhenBenchmarkOverrideIsAbsent() {
        ReactivePerformanceArguments arguments = ReactivePerformanceArguments.parse(new String[]{
                "--postgresql-url", "r2dbc:postgresql://local/test",
                "--output", "target/metrics.json",
                "--summary", "target/summary.md",
                "--git-commit", "abcdef1"
        });

        assertEquals(SqlExecutionOptions.safeDefaults().fetchSize(),
                     ReactivePerformanceDatabaseRunner.queryOptions(arguments).fetchSize());
        assertEquals(null, arguments.reportParameters().fetchSizeOverride());
        assertEquals(SqlExecutionOptions.safeDefaults().fetchSize(),
                     arguments.reportParameters().effectiveFetchSize());
    }

    /** 四库性能门禁必须使用数据库自己的标识符、绑定标记和版本查询，不能依赖临时脚本。 */
    @Test
    void buildsOracleAndSqlServerPerformanceTargets() {
        ReactivePerformanceArguments arguments = ReactivePerformanceArguments.parse(new String[]{
                "--oracle-url", "r2dbc:oracle://local/test",
                "--sqlserver-url", "r2dbc:mssql://local/test",
                "--output", "target/metrics.json",
                "--summary", "target/summary.md",
                "--git-commit", "abcdef1"
        });

        List<ReactivePerformanceTarget> targets = arguments.targets();
        assertEquals(List.of("oracle", "sqlserver"), targets.stream().map(ReactivePerformanceTarget::key).toList());
        assertEquals("?", targets.get(0).bindMarker());
        assertTrue(targets.get(0).createSql().contains("varchar2(128)"));
        assertTrue(targets.get(0).versionSql().contains("product_component_version"));
        assertFalse(targets.get(0).versionSql().contains("product like"));
        assertEquals("@P0", targets.get(1).bindMarker());
        assertTrue(targets.get(1).table().startsWith("["));
        assertTrue(targets.get(1).versionSql().contains("serverproperty"));
    }

    /** 正式报告必须绑定当前 HEAD 和实际加载 class，调用方文本标签不能伪造候选身份。 */
    @Test
    void capturesVerifiableRunIdentityAndRejectsMismatchedCommitLabel() {
        DatabasePerformanceReport.RunIdentity identity = BenchmarkRunIdentity.capture();

        assertTrue(identity.gitHead().matches("[0-9a-f]{40}"));
        assertTrue(identity.trackedDiffSha256().matches("[0-9a-f]{64}"));
        assertTrue(identity.classpathSha256().matches("[0-9a-f]{64}"));
        assertTrue(identity.benchmarkClassSha256().matches("[0-9a-f]{64}"));
        assertTrue(identity.rdbClassSha256().matches("[0-9a-f]{64}"));
        assertNotNull(identity.garbageCollectors());
        BenchmarkRunIdentity.requireCommitLabel(identity, identity.gitHead().substring(0, 7));
        assertThrows(IllegalArgumentException.class,
                     () -> BenchmarkRunIdentity.requireCommitLabel(identity, "feature-2.0.0"));
    }

    @Test
    void rejectsUnknownOptionsAndAllowsBoundedPoolUnderHighRequestConcurrency() {
        assertThrows(IllegalArgumentException.class,
                     () -> ReactivePerformanceArguments.parse(new String[]{
                             "--mysql-url", "r2dbc:mysql://local/test",
                             "--output", "target/metrics.json",
                             "--summary", "target/summary.md",
                             "--git-commit", "abcdef1",
                             "--unknown", "value"
                     }));
        ReactivePerformanceArguments highConcurrency = ReactivePerformanceArguments.parse(new String[]{
                "--mysql-url", "r2dbc:mysql://local/test",
                "--output", "target/metrics.json",
                "--summary", "target/summary.md",
                "--git-commit", "abcdef1",
                "--pool-size", "64",
                "--query-concurrency", "10001"
        });

        assertEquals(64, highConcurrency.reportParameters().poolSize());
        assertEquals(10001, highConcurrency.reportParameters().queryConcurrency());
    }

    @Test
    void acceptsOnlyKnownScenarioNamesAndKeepsCallerOrder() {
        ReactivePerformanceArguments arguments = ReactivePerformanceArguments.parse(new String[]{
                "--mysql-url", "r2dbc:mysql://local/test",
                "--output", "target/metrics.json",
                "--summary", "target/summary.md",
                "--git-commit", "abcdef1",
                "--scenarios", "updateById,transactionalUpdateBatch,rawQueryById,queryById"
        });

        assertEquals(List.of("updateById", "transactionalUpdateBatch", "rawQueryById", "queryById"),
                     arguments.scenarioNames());
        assertThrows(IllegalArgumentException.class,
                     () -> ReactivePerformanceArguments.parse(new String[]{
                             "--mysql-url", "r2dbc:mysql://local/test",
                             "--output", "target/metrics.json",
                             "--summary", "target/summary.md",
                             "--git-commit", "abcdef1",
                             "--scenarios", "updateById,rawSql"
                     }));
    }

    /** 原生对照必须消费结果并归还同一个池连接，不能用不完整的驱动调用制造虚假优势。 */
    @Test
    void rawQueryByIdKeepsDriverFetchDefaultAndCloses() {
        AtomicReference<Object> bound = new AtomicReference<>();
        AtomicInteger fetchSize = new AtomicInteger(-1);
        AtomicInteger mapped = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        ConnectionFactory factory = rawQueryConnectionFactory(bound, fetchSize, mapped, closes);

        ReactivePerformanceScenarioRunner.rawQueryById(
                factory, "TEST_TABLE", "$1", new AtomicLong(4), 100, 0).block();

        assertEquals(5L, bound.get());
        assertEquals(-1, fetchSize.get());
        assertEquals(1, mapped.get());
        assertEquals(1, closes.get());
    }

    /** 显式正数 fetchSize 必须继续传给驱动，保证大结果流诊断场景与 ORM 配置一致。 */
    @Test
    void rawQueryByIdAppliesPositiveFetchSize() {
        AtomicReference<Object> bound = new AtomicReference<>();
        AtomicInteger fetchSize = new AtomicInteger(-1);
        ConnectionFactory factory = rawQueryConnectionFactory(
                bound, fetchSize, new AtomicInteger(), new AtomicInteger());

        ReactivePerformanceScenarioRunner.rawQueryById(
                factory, "TEST_TABLE", "$1", new AtomicLong(), 100, 256).block();

        assertEquals(256, fetchSize.get());
    }

    @Test
    void calculatesMySqlFileWaitDeltasAcrossDriverColumnNameCases() {
        MySqlWaitSnapshot before = MySqlWaitSnapshot.fromRows(List.of(
                        Map.of("EVENT_NAME", "wait/io/file/sql/binlog",
                               "COUNT_STAR", 10L, "SUM_TIMER_WAIT", 2_000_000_000L,
                               "MAX_TIMER_WAIT", 800_000_000L),
                        Map.of("event_name", "wait/io/file/innodb/innodb_log_file",
                               "count_star", 20L, "sum_timer_wait", 3_000_000_000L,
                               "max_timer_wait", 900_000_000L)));
        MySqlWaitSnapshot after = MySqlWaitSnapshot.fromRows(List.of(
                        Map.of("event_name", "wait/io/file/sql/binlog",
                               "count_star", 14L, "sum_timer_wait", 5_000_000_000L,
                               "max_timer_wait", 1_200_000_000L),
                        Map.of("EVENT_NAME", "wait/io/file/innodb/innodb_log_file",
                               "COUNT_STAR", 25L, "SUM_TIMER_WAIT", 7_000_000_000L,
                               "MAX_TIMER_WAIT", 1_500_000_000L)));

        MySqlWaitSnapshot delta = after.minus(before);

        assertEquals(4L, delta.binlogCount());
        assertEquals(3.0, delta.binlogWaitMillis());
        assertEquals(5L, delta.redoCount());
        assertEquals(4.0, delta.redoWaitMillis());
        assertEquals(1.2, delta.binlogMaxMillis());
        assertEquals(1.5, delta.redoMaxMillis());
    }

    @Test
    void rollsBackAndClosesWhenOneUpdateInTransactionDoesNotMatch() {
        AtomicInteger updates = new AtomicInteger();
        AtomicInteger commits = new AtomicInteger();
        AtomicInteger rollbacks = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        ConnectionFactory factory = transactionConnectionFactory(updates, commits, rollbacks, closes);

        assertThrows(IllegalStateException.class,
                     () -> ReactivePerformanceScenarioRunner
                        .transactionalUpdateBatch(factory, "TEST_TABLE", "?", 0, 100)
                             .block());

        assertEquals(2, updates.get());
        assertEquals(0, commits.get());
        assertEquals(1, rollbacks.get());
        assertEquals(1, closes.get());
    }

    private static ConnectionFactory transactionConnectionFactory(AtomicInteger updates,
                                                                  AtomicInteger commits,
                                                                  AtomicInteger rollbacks,
                                                                  AtomicInteger closes) {
        Result result = (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (proxy, method, arguments) -> {
                    if ("getRowsUpdated".equals(method.getName())) {
                        return reactor.core.publisher.Mono.just(updates.incrementAndGet() == 2 ? 0L : 1L);
                    }
                    throw new UnsupportedOperationException("test result does not implement " + method.getName());
                });
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind" -> proxy;
                    case "execute" -> reactor.core.publisher.Mono.just(result);
                    default -> throw new UnsupportedOperationException(
                            "test statement does not implement " + method.getName());
                });
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "beginTransaction" -> reactor.core.publisher.Mono.empty();
                    case "commitTransaction" -> reactor.core.publisher.Mono.fromRunnable(commits::incrementAndGet);
                    case "rollbackTransaction" -> reactor.core.publisher.Mono.fromRunnable(
                            rollbacks::incrementAndGet);
                    case "close" -> reactor.core.publisher.Mono.fromRunnable(closes::incrementAndGet);
                    case "createStatement" -> statement;
                    default -> throw new UnsupportedOperationException(
                            "test connection does not implement " + method.getName());
                });
        return new ConnectionFactory() {
            @Override
            public reactor.core.publisher.Mono<? extends Connection> create() {
                return reactor.core.publisher.Mono.just(connection);
            }

            @Override
            public io.r2dbc.spi.ConnectionFactoryMetadata getMetadata() {
                return () -> "transaction-test";
            }
        };
    }

    private static ConnectionFactory rawQueryConnectionFactory(AtomicReference<Object> bound,
                                                               AtomicInteger fetchSize,
                                                               AtomicInteger mapped,
                                                               AtomicInteger closes) {
        Result result = (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(), new Class<?>[]{Result.class},
                (proxy, method, arguments) -> {
                    if ("map".equals(method.getName())) {
                        mapped.incrementAndGet();
                        return reactor.core.publisher.Mono.just(1L);
                    }
                    throw new UnsupportedOperationException("test result does not implement " + method.getName());
                });
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(), new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind" -> {
                        bound.set(arguments[1]);
                        yield proxy;
                    }
                    case "fetchSize" -> {
                        fetchSize.set((Integer) arguments[0]);
                        yield proxy;
                    }
                    case "execute" -> reactor.core.publisher.Mono.just(result);
                    default -> throw new UnsupportedOperationException(
                            "test statement does not implement " + method.getName());
                });
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "createStatement" -> statement;
                    case "close" -> reactor.core.publisher.Mono.fromRunnable(closes::incrementAndGet);
                    default -> throw new UnsupportedOperationException(
                            "test connection does not implement " + method.getName());
                });
        return new ConnectionFactory() {
            @Override
            public reactor.core.publisher.Mono<? extends Connection> create() {
                return reactor.core.publisher.Mono.just(connection);
            }

            @Override
            public io.r2dbc.spi.ConnectionFactoryMetadata getMetadata() {
                return () -> "raw-query-test";
            }
        };
    }
}
