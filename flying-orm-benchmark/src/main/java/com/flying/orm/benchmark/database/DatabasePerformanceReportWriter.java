package com.flying.orm.benchmark.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Objects;

/** 把同一份性能结果写成机器可读 JSON 和方便人工复核的中文摘要。 */
final class DatabasePerformanceReportWriter {

    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private DatabasePerformanceReportWriter() {
    }

    static void write(DatabasePerformanceReport report, Path jsonFile, Path markdownFile) throws IOException {
        DatabasePerformanceReport safeReport = Objects.requireNonNull(report,
                                                                       "database performance report must not be null");
        Path safeJson = Objects.requireNonNull(jsonFile, "database performance JSON path must not be null");
        Path safeMarkdown = Objects.requireNonNull(markdownFile,
                                                   "database performance Markdown path must not be null");
        createParent(safeJson);
        createParent(safeMarkdown);
        JSON.writeValue(safeJson.toFile(), safeReport);
        Files.writeString(safeMarkdown, markdown(safeReport), StandardCharsets.UTF_8);
    }

    private static String markdown(DatabasePerformanceReport report) {
        StringBuilder text = new StringBuilder(2048);
        text.append("# flying-orm 真实数据库性能结果\n\n")
            .append("- 运行编号：`").append(report.runId()).append("`\n")
            .append("- Git commit：`").append(report.gitCommit()).append("`\n")
            .append("- 状态：**").append(report.status()).append("**\n")
            .append("- 时间：").append(report.startedAt()).append(" 至 ").append(report.completedAt()).append("\n\n")
            .append("## 固定参数\n\n")
            .append("连接池 ").append(report.parameters().poolSize())
            .append("，查询/更新并发 ").append(report.parameters().queryConcurrency())
            .append("，批量外层并发 ").append(report.parameters().batchConcurrency())
            .append("，每批 ").append(report.parameters().batchSize())
            .append(" 行，预热 ").append(report.parameters().warmupSeconds())
            .append(" 秒，测量 ").append(report.parameters().measurementSeconds()).append(" 秒。\n\n");

        for (DatabasePerformanceReport.DatabaseResult database : report.databases()) {
            text.append("## ").append(database.database()).append("\n\n")
                .append("数据库版本：").append(database.databaseVersion()).append("；驱动：")
                .append(database.driver()).append("；连接池：").append(database.poolVersion()).append("。\n\n")
                .append("| 场景 | 状态 | ops/s | rows/s | P50 ms | P95 ms | P99 ms | 错误率 | 峰值连接 | CPU | 峰值堆 |\n")
                .append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
            for (DatabasePerformanceReport.ScenarioResult scenario : database.scenarios()) {
                text.append("| ").append(scenario.scenario())
                    .append(" | ").append(scenario.status())
                    .append(" | ").append(number(scenario.operationsPerSecond()))
                    .append(" | ").append(number(scenario.rowsPerSecond()))
                    .append(" | ").append(number(scenario.p50Millis()))
                    .append(" | ").append(number(scenario.p95Millis()))
                    .append(" | ").append(number(scenario.p99Millis()))
                    .append(" | ").append(percent(scenario.errorRate()))
                    .append(" | ").append(scenario.peakAcquiredConnections())
                    .append(" | ").append(number(scenario.averageProcessCpuPercent())).append("%")
                    .append(" | ").append(bytes(scenario.peakHeapBytes()))
                    .append(" |\n");
            }
            appendPhaseLatency(text, database.scenarios());
            text.append("\n最终池状态：allocated=").append(database.finalAllocatedConnections())
                .append("，acquired=").append(database.finalAcquiredConnections())
                .append("，idle=").append(database.finalIdleConnections())
                .append("，pending=").append(database.finalPendingAcquires()).append("。\n\n");
        }

        text.append("> CPU 是负载器 Java 进程平均占用，不包含 Docker 数据库进程；堆内存是 JVM 内存池峰值，")
            .append("不等于整个进程 RSS。本结果只代表记录中的本机与固定环境。\n");
        return text.toString();
    }

    private static void appendPhaseLatency(StringBuilder text,
                                           java.util.List<DatabasePerformanceReport.ScenarioResult> scenarios) {
        boolean hasDiagnostics = scenarios.stream().anyMatch(scenario -> scenario.phaseLatency() != null);
        if (!hasDiagnostics) {
            return;
        }
        text.append("\n### 操作阶段延迟\n\n")
            .append("| 场景 | 阶段 | 样本数 | P50 ms | P95 ms | P99 ms | 最大值 ms |\n")
            .append("| --- | --- | ---: | ---: | ---: | ---: | ---: |\n");
        for (DatabasePerformanceReport.ScenarioResult scenario : scenarios) {
            DatabasePerformanceReport.PhaseLatency latency = scenario.phaseLatency();
            if (latency == null) {
                continue;
            }
            appendPhase(text, scenario.scenario(), "连接获取", latency.acquire());
            appendPhase(text, scenario.scenario(), "执行及提交", latency.executeAndCommit());
            appendPhase(text, scenario.scenario(), "连接归还", latency.release());
            appendPhase(text, scenario.scenario(), "操作总计", latency.total());
        }
        text.append("\n> 执行及提交按每条操作的总耗时减去该条连接获取和归还耗时后再统计，不能用几个分位数相减。\n");
    }

    private static void appendPhase(StringBuilder text,
                                    String scenario,
                                    String phase,
                                    DatabasePerformanceReport.Latency latency) {
        text.append("| ").append(scenario)
            .append(" | ").append(phase)
            .append(" | ").append(latency.samples())
            .append(" | ").append(number(latency.p50Millis()))
            .append(" | ").append(number(latency.p95Millis()))
            .append(" | ").append(number(latency.p99Millis()))
            .append(" | ").append(number(latency.maxMillis()))
            .append(" |\n");
    }

    private static void createParent(Path file) throws IOException {
        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    private static String number(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static String percent(double value) {
        return String.format(Locale.ROOT, "%.4f%%", value * 100.0);
    }

    private static String bytes(long value) {
        return String.format(Locale.ROOT, "%.2f MiB", value / 1024.0 / 1024.0);
    }
}
