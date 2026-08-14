package com.flying.orm.benchmark.database;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.List;
import java.util.Map;

/** 统一生成报告中的环境和失败结果，避免主 runner 夹杂脱敏细节。 */
final class JdbcPerformanceReportSupport {

    private JdbcPerformanceReportSupport() {
    }

    static DatabasePerformanceReport.Environment environment() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        return new DatabasePerformanceReport.Environment(
                System.getProperty("java.version", "unknown"),
                System.getProperty("java.vendor", "unknown"),
                System.getProperty("os.name", "unknown") + " " + System.getProperty("os.version", ""),
                os.getName(),
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().maxMemory());
    }

    static DatabasePerformanceReport.DatabaseResult failedDatabase(String database, Throwable error) {
        ReactivePerformanceReportSupport.rethrowFatal(error);
        DatabasePerformanceReport.ScenarioResult scenario = new DatabasePerformanceReport.ScenarioResult(
                "setup", DatabasePerformanceReport.Status.FAILED, 1, 0, 0, 0, 0, 0, 1,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                Map.of(type(error), 1L), 0, 0, 0, 0.0, 0L, null);
        return new DatabasePerformanceReport.DatabaseResult(
                database, "unavailable", "unavailable", "HikariCP 7.0.2",
                DatabasePerformanceReport.Status.FAILED, 0, 0, 0, 0, List.of(scenario));
    }

    static String type(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getClass() == RuntimeException.class) {
            current = current.getCause();
        }
        String name = current.getClass().getSimpleName();
        return name == null || name.isBlank() ? "UnknownFailure" : name;
    }

    /** MySQL 驱动会启动一个 JVM 级清理线程；命令行基准结束时主动收尾，普通应用生命周期不受这里影响。 */
    static void shutdownOptionalDriverThreads() {
        try {
            Class<?> cleanup = Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread");
            cleanup.getMethod("checkedShutdown").invoke(null);
        } catch (ClassNotFoundException ignored) {
            // 只运行 PostgreSQL 时 classpath 本来就没有 MySQL 驱动。
        } catch (ReflectiveOperationException error) {
            System.err.println("JDBC benchmark driver cleanup failed: " + type(error));
        }
    }
}
