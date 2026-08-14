package com.flying.orm.benchmark.database;

import reactor.core.Exceptions;

import java.lang.management.ManagementFactory;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** 组装与具体数据库执行无关的环境信息和启动失败报告。 */
final class ReactivePerformanceReportSupport {

    private ReactivePerformanceReportSupport() {
    }

    static DatabasePerformanceReport.Environment environment() {
        String os = System.getProperty("os.name") + " " + System.getProperty("os.version")
                + " " + System.getProperty("os.arch");
        String cpu = System.getenv().getOrDefault(
                "PROCESSOR_IDENTIFIER", ManagementFactory.getOperatingSystemMXBean().getName());
        return new DatabasePerformanceReport.Environment(
                System.getProperty("java.version"), System.getProperty("java.vendor"), os, cpu,
                Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().maxMemory());
    }

    static DatabasePerformanceReport.DatabaseResult failedDatabase(String database, Throwable error) {
        rethrowFatal(error);
        String failure = error.getClass().getSimpleName().isBlank()
                ? error.getClass().getName() : error.getClass().getSimpleName();
        DatabasePerformanceReport.ScenarioResult setup = new DatabasePerformanceReport.ScenarioResult(
                "setup", DatabasePerformanceReport.Status.FAILED, 1, 1, 0, 1, 0, 0, 1,
                0, 0, 1, 0, 0, 0, 0, Map.of(failure, 1L),
                0, 0, 0, 0, 0, null);
        return new DatabasePerformanceReport.DatabaseResult(
                database, "unavailable", "unavailable", ReactivePerformanceDatabaseRunner.poolVersion(),
                DatabasePerformanceReport.Status.FAILED, 0, 0, 0, List.of(setup));
    }

    /** 在 Reactor 操作符内先用普通错误信号携带 fatal，避免框架把直接抛出的致命错误记录后丢弃。 */
    static Throwable errorSignal(Throwable error) {
        return findFatal(error) == null
                ? error : new IllegalStateException("database performance operation failed fatally", error);
    }

    /** Reactor 致命错误及被驱动包装的 JVM 致命错误都必须原样离开性能工具。 */
    static void rethrowFatal(Throwable root) {
        Throwable fatal = findFatal(root);
        if (fatal != null) {
            Exceptions.throwIfFatal(fatal);
        }
    }

    /** 按对象身份遍历 cause/suppressed，外来异常图即使已有环也不会递归失控。 */
    private static Throwable findFatal(Throwable root) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (Exceptions.isFatal(current)) {
                return current;
            }
            if (current.getCause() != null) {
                pending.add(current.getCause());
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return null;
    }
}
