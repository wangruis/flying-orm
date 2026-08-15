package com.flying.orm.benchmark.database;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * 在真实 MySQL/PostgreSQL 上运行固定参数的响应式性能场景。
 *
 * <p>这是 benchmark 模块的独立命令行入口，不是 ORM 运行时。参数解析、数据库生命周期、场景执行和资源采样
 * 分别交给包内协作者；这里仅决定执行顺序、汇总状态并写报告。连接串不会进入报告模型。</p>
 *
 * @author wangr
 * @date 2026-08-02
 * @version v2.0.0
 */
public final class RealDatabasePerformanceRunner {

    private static final DateTimeFormatter RUN_ID_TIME = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                                                                        .withZone(ZoneOffset.UTC);

    private RealDatabasePerformanceRunner() {
    }

    public static void main(String[] args) throws Exception {
        ReactivePerformanceArguments arguments = ReactivePerformanceArguments.parse(args);
        DatabasePerformanceReport.RunIdentity runIdentity = BenchmarkRunIdentity.capture();
        BenchmarkRunIdentity.requireCommitLabel(runIdentity, arguments.gitCommit);
        Instant started = Instant.now();
        String runId = arguments.runId == null
                ? RUN_ID_TIME.format(started) + "-" + arguments.gitCommit : arguments.runId;
        List<DatabasePerformanceReport.DatabaseResult> databases = new ArrayList<>();
        for (ReactivePerformanceTarget target : arguments.targets()) {
            try {
                databases.add(ReactivePerformanceDatabaseRunner.run(target, arguments));
            } catch (Throwable error) {
                databases.add(ReactivePerformanceReportSupport.failedDatabase(target.name(), error));
            }
        }
        DatabasePerformanceReport.Status status = databases.stream()
                .allMatch(result -> result.status() == DatabasePerformanceReport.Status.PASSED)
                ? DatabasePerformanceReport.Status.PASSED : DatabasePerformanceReport.Status.FAILED;
        DatabasePerformanceReport report = new DatabasePerformanceReport(
                1, runId, arguments.gitCommit, runIdentity, started.toString(), Instant.now().toString(), status,
                ReactivePerformanceReportSupport.environment(), arguments.reportParameters(), databases);
        DatabasePerformanceReportWriter.write(report, arguments.output, arguments.summary);
        if (status == DatabasePerformanceReport.Status.FAILED) {
            throw new IllegalStateException("real database performance run failed; inspect the generated report");
        }
    }
}
