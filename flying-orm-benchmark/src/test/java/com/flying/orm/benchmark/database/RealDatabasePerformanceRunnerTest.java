package com.flying.orm.benchmark.database;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void rejectsUnknownOptionsAndConcurrencyThatExceedsPool() {
        assertThrows(IllegalArgumentException.class,
                     () -> ReactivePerformanceArguments.parse(new String[]{
                             "--mysql-url", "r2dbc:mysql://local/test",
                             "--output", "target/metrics.json",
                             "--summary", "target/summary.md",
                             "--git-commit", "abcdef1",
                             "--unknown", "value"
                     }));
        assertThrows(IllegalArgumentException.class,
                     () -> ReactivePerformanceArguments.parse(new String[]{
                             "--mysql-url", "r2dbc:mysql://local/test",
                             "--output", "target/metrics.json",
                             "--summary", "target/summary.md",
                             "--git-commit", "abcdef1",
                             "--pool-size", "4",
                             "--query-concurrency", "8"
                     }));
    }

    @Test
    void acceptsOnlyKnownScenarioNamesAndKeepsCallerOrder() {
        ReactivePerformanceArguments arguments = ReactivePerformanceArguments.parse(new String[]{
                "--mysql-url", "r2dbc:mysql://local/test",
                "--output", "target/metrics.json",
                "--summary", "target/summary.md",
                "--git-commit", "abcdef1",
                "--scenarios", "updateById,transactionalUpdateBatch,queryById"
        });

        assertEquals(List.of("updateById", "transactionalUpdateBatch", "queryById"),
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
}
