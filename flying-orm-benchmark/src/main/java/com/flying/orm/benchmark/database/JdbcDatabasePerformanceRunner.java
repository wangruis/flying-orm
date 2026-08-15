package com.flying.orm.benchmark.database;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.result.DynamicRow;
import com.zaxxer.hikari.HikariDataSource;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * V2 原生 JDBC 的四库真实性能入口。
 *
 * <p>只有明确配置 JDBC URL 的目标才会运行，未配置的数据库直接跳过，不会自行寻找或启动数据库。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class JdbcDatabasePerformanceRunner {

    private static final String HIKARI_VERSION = "HikariCP 7.0.2";

    private JdbcDatabasePerformanceRunner() {
    }

    public static void main(String[] args) throws Exception {
        try {
            run(JdbcPerformanceArguments.parse(args));
        } finally {
            JdbcPerformanceReportSupport.shutdownOptionalDriverThreads();
        }
    }

    private static void run(JdbcPerformanceArguments arguments) throws Exception {
        DatabasePerformanceReport.RunIdentity runIdentity = BenchmarkRunIdentity.capture();
        BenchmarkRunIdentity.requireCommitLabel(runIdentity, arguments.gitCommit);
        Instant started = Instant.now();
        List<DatabasePerformanceReport.DatabaseResult> databases = new ArrayList<>();
        if (!arguments.mysqlUrl.isBlank()) databases.add(runTarget(JdbcPerformanceTarget.mysql(arguments), arguments));
        else System.out.println("JDBC target MySQL skipped: URL is not configured");
        if (!arguments.postgresqlUrl.isBlank()) databases.add(runTarget(JdbcPerformanceTarget.postgresql(arguments), arguments));
        else System.out.println("JDBC target PostgreSQL skipped: URL is not configured");
        if (!arguments.oracleUrl.isBlank()) databases.add(runTarget(JdbcPerformanceTarget.oracle(arguments), arguments));
        else System.out.println("JDBC target Oracle skipped: URL is not configured");
        if (!arguments.sqlserverUrl.isBlank()) databases.add(runTarget(JdbcPerformanceTarget.sqlserver(arguments), arguments));
        else System.out.println("JDBC target SQL Server skipped: URL is not configured");
        DatabasePerformanceReport.Status status = databases.stream().allMatch(result ->
                result.status() == DatabasePerformanceReport.Status.PASSED)
                ? DatabasePerformanceReport.Status.PASSED : DatabasePerformanceReport.Status.FAILED;
        DatabasePerformanceReport report = new DatabasePerformanceReport(
                1, arguments.runId, arguments.gitCommit, runIdentity,
                started.toString(), Instant.now().toString(), status,
                JdbcPerformanceReportSupport.environment(), arguments.reportParameters(), databases);
        DatabasePerformanceReportWriter.write(report, arguments.output, arguments.summary);
        if (status == DatabasePerformanceReport.Status.FAILED) {
            throw new IllegalStateException("JDBC database performance run failed; inspect the generated report");
        }
    }

    private static DatabasePerformanceReport.DatabaseResult runTarget(JdbcPerformanceTarget target,
                                                                      JdbcPerformanceArguments arguments) {
        HikariDataSource dataSource = null;
        FlyingOrmClients clients = null;
        String stage = "pool";
        try {
            dataSource = target.openPool(arguments.poolSize);
            stage = "client";
            clients = FlyingOrmClients.builder(dataSource).build();
            stage = "metadata";
            JdbcDatabaseInfo info = databaseInfo(clients);
            DynamicForm form = target.form();
            stage = "prepare";
            prepare(target, clients, form, arguments.seedRows);
            stage = "scenarios";
            List<DatabasePerformanceReport.ScenarioResult> scenarios = runScenarios(
                    target, clients, dataSource, form, arguments);
            stage = "pool-settle";
            JdbcPoolState finalPool = settle(dataSource);
            DatabasePerformanceReport.Status status = scenarios.stream().allMatch(scenario ->
                    scenario.status() == DatabasePerformanceReport.Status.PASSED)
                    && finalPool.active() == 0 && finalPool.pending() == 0
                    ? DatabasePerformanceReport.Status.PASSED : DatabasePerformanceReport.Status.FAILED;
            System.out.printf("JDBC target=%s pool final active=%d idle=%d pending=%d%n",
                              target.name(), finalPool.active(), finalPool.idle(), finalPool.pending());
            return new DatabasePerformanceReport.DatabaseResult(
                    target.name(), info.databaseVersion(), info.driver(), HIKARI_VERSION, status,
                    finalPool.total(), finalPool.active(), finalPool.pending(), finalPool.idle(), scenarios);
        } catch (Throwable error) {
            printFailure(target, stage, error);
            return JdbcPerformanceReportSupport.failedDatabase(target.name(), error);
        } finally {
            if (clients != null) {
                try {
                    clients.syncExecutor().rowsUpdated(SqlRequest.nativeSql(target.dropSql(), List.of()));
                } catch (RuntimeException ignored) {
                    // 业务场景已经结束，清理失败由连接池关闭兜底，不把凭据或 SQL 异常写进报告。
                }
                clients.close();
            }
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }

    /** 只打印数据库分类和标准错误码，连接地址、账号、SQL 与参数都不会进入控制台。 */
    private static void printFailure(JdbcPerformanceTarget target, String stage, Throwable error) {
        if (error instanceof RdbException failure) {
            System.out.printf("JDBC target=%s stage=%s failed=%s kind=%s sqlState=%s errorCode=%s%n",
                              target.name(), stage, failure.getClass().getSimpleName(), failure.kind(),
                              failure.sqlState(), failure.errorCode());
            return;
        }
        System.out.printf("JDBC target=%s stage=%s failed=%s%n",
                          target.name(), stage, JdbcPerformanceReportSupport.type(error));
    }

    private static List<DatabasePerformanceReport.ScenarioResult> runScenarios(
            JdbcPerformanceTarget target,
            FlyingOrmClients clients,
            HikariDataSource dataSource,
            DynamicForm form,
            JdbcPerformanceArguments arguments) {
        JdbcPerformanceScenarioRunner runner = new JdbcPerformanceScenarioRunner();
        AtomicLong queryIds = new AtomicLong();
        AtomicLong batchIds = new AtomicLong(arguments.seedRows + 1L);
        List<DatabasePerformanceReport.ScenarioResult> results = new ArrayList<>();
        for (JdbcPerformanceScenario scenario : arguments.scenarios) {
            switch (scenario) {
                case QUERY_BY_ID -> results.add(runner.run(
                        scenario.name, arguments.queryConcurrency, arguments, dataSource,
                        () -> queryById(clients, target, Math.floorMod(queryIds.getAndIncrement(), arguments.seedRows) + 1L)));
                case UPDATE_BY_ID -> results.add(runner.run(
                        scenario.name, arguments.queryConcurrency, arguments, dataSource,
                        () -> updateById(clients, target, Math.floorMod(queryIds.getAndIncrement(), arguments.seedRows) + 1L)));
                case ATOMIC_BATCH_INSERT -> results.add(runner.run(
                        scenario.name, arguments.batchConcurrency, arguments, dataSource,
                        () -> batchInsert(clients, target, form, batchIds.getAndAdd(arguments.batchSize),
                                          arguments.batchSize, BatchWriteOptions.atomic(arguments.batchSize))));
                case INDEPENDENT_BATCH_INSERT -> results.add(runner.run(
                        scenario.name, arguments.batchConcurrency, arguments, dataSource,
                        () -> batchInsert(clients, target, form, batchIds.getAndAdd(arguments.batchSize),
                                          arguments.batchSize,
                                          BatchWriteOptions.independent(arguments.independentChunkSize, 1))));
            }
        }
        return List.copyOf(results);
    }

    private static void prepare(JdbcPerformanceTarget target,
                                FlyingOrmClients clients,
                                DynamicForm form,
                                int seedRows) {
        try {
            clients.syncExecutor().rowsUpdated(SqlRequest.nativeSql(target.dropSql(), List.of()));
        } catch (RdbException ignored) {
            // Oracle 没有跨版本稳定的 DROP TABLE IF EXISTS；缺表时继续，其他失败会在 CREATE 阶段明确暴露。
        }
        System.out.printf("JDBC target=%s prepare=drop-complete%n", target.name());
        clients.syncExecutor().rowsUpdated(SqlRequest.nativeSql(target.createSql(), List.of()));
        System.out.printf("JDBC target=%s prepare=create-complete%n", target.name());
        BatchWriteResult result = clients.syncForms().writeBatch(
                BatchSpec.insert(form, Flux.range(1, seedRows).map(id -> {
                    long rowId = id.longValue();
                    return Map.of("id", rowId, "name", "seed-" + rowId, "value", rowId);
                }))
                         .withOptions(BatchWriteOptions.atomic(Math.min(500, seedRows))));
        if (result.status() != BatchWriteResult.Status.COMMITTED || result.inputCount() != seedRows
                || result.affectedRows() != seedRows) {
            throw new IllegalStateException("JDBC benchmark seed batch did not commit every row");
        }
        System.out.printf("JDBC target=%s prepare=seed-complete rows=%d%n", target.name(), seedRows);
    }

    private static long queryById(FlyingOrmClients clients, JdbcPerformanceTarget target, long id) {
        List<DynamicRow> rows = clients.syncExecutor().query(new SqlRequest(target.querySql(), List.of(id)));
        if (rows.size() != 1) {
            throw new IllegalStateException("JDBC benchmark query did not return one row");
        }
        return 1;
    }

    private static long updateById(FlyingOrmClients clients, JdbcPerformanceTarget target, long id) {
        long updated = clients.syncExecutor().rowsUpdated(new SqlRequest(target.updateSql(), List.of(id)));
        if (updated != 1) {
            throw new IllegalStateException("JDBC benchmark update did not affect one row");
        }
        return updated;
    }

    private static long batchInsert(FlyingOrmClients clients,
                                    JdbcPerformanceTarget target,
                                    DynamicForm form,
                                    long firstId,
                                    int batchSize,
                                    BatchWriteOptions options) {
        List<Map<String, Object>> rows = new ArrayList<>(batchSize);
        for (int offset = 0; offset < batchSize; offset++) {
            long id = firstId + offset;
            rows.add(Map.of("id", id, "name", "batch-" + id, "value", id));
        }
        BatchWriteResult result = clients.syncForms().writeBatch(
                BatchSpec.insert(form, Flux.fromIterable(rows))
                         .withOptions(options));
        if (result.status() != BatchWriteResult.Status.COMMITTED || result.inputCount() != batchSize
                || result.affectedRows() != batchSize) {
            throw new IllegalStateException("JDBC benchmark batch did not commit every row");
        }
        return batchSize;
    }

    private static JdbcDatabaseInfo databaseInfo(FlyingOrmClients clients) {
        return clients.jdbcAdvanced().metadata(metadata -> new JdbcDatabaseInfo(
                text(metadata.getDatabaseProductVersion(), "unknown"),
                text(metadata.getDriverName(), "JDBC") + " " + text(metadata.getDriverVersion(), "unknown")));
    }

    private static JdbcPoolState settle(HikariDataSource dataSource) {
        long deadline = System.nanoTime() + 5_000_000_000L;
        JdbcPoolState state;
        do {
            state = JdbcPoolSampler.current(dataSource);
            if (state.active() == 0 && state.pending() == 0) {
                return state;
            }
            try {
                Thread.sleep(10L);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                return state;
            }
        } while (System.nanoTime() < deadline);
        return state;
    }

    private static String text(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private record JdbcDatabaseInfo(String databaseVersion, String driver) {
    }
}
