package com.flying.orm.benchmark.database;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 报告测试只检查稳定字段、状态一致性和脱敏边界。 */
class DatabasePerformanceReportWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void writesJsonAndChineseSummaryWithoutConnectionSecrets() throws Exception {
        DatabasePerformanceReport report = report();
        Path json = tempDir.resolve("metrics.json");
        Path markdown = tempDir.resolve("summary.md");

        DatabasePerformanceReportWriter.write(report, json, markdown);

        String jsonText = Files.readString(json);
        String markdownText = Files.readString(markdown);
        assertTrue(jsonText.contains("\"scenario\" : \"queryById\""));
        assertTrue(jsonText.contains("\"phaseLatency\""));
        assertTrue(jsonText.contains("\"executeAndCommit\""));
        assertTrue(markdownText.contains("真实数据库性能结果"));
        assertTrue(markdownText.contains("连接获取"));
        assertTrue(markdownText.contains("执行及提交"));
        assertFalse((jsonText + markdownText).contains("r2dbc:mysql"));
        assertFalse((jsonText + markdownText).toLowerCase().contains("password"));
    }

    @Test
    void rejectsPassedScenarioThatContainsFailures() {
        assertThrows(IllegalArgumentException.class,
                     () -> new DatabasePerformanceReport.ScenarioResult(
                             "queryById", DatabasePerformanceReport.Status.PASSED, 4,
                             10, 9, 1, 9, 0, 1000,
                             9, 9, 0.1, 1, 2, 3, 4,
                             Map.of("planned", 1L), 4, 4, 0, 20, 1024, null));
    }

    private static DatabasePerformanceReport report() {
        DatabasePerformanceReport.ScenarioResult scenario = new DatabasePerformanceReport.ScenarioResult(
                "queryById", DatabasePerformanceReport.Status.PASSED, 4,
                100, 100, 0, 100, 0, 1000,
                100, 100, 0, 1, 2, 3, 4,
                Map.of(), 4, 4, 0, 25, 16 * 1024 * 1024L,
                new DatabasePerformanceReport.PhaseLatency(
                        latency(100, 0.1, 0.2, 0.3, 0.4),
                        latency(100, 0.5, 0.8, 1.0, 1.4),
                        latency(100, 0.1, 0.2, 0.2, 0.3),
                        latency(100, 0.8, 1.2, 1.5, 2.0)));
        DatabasePerformanceReport.DatabaseResult database = new DatabasePerformanceReport.DatabaseResult(
                "MySQL", "8.4.10", "r2dbc-mysql", "r2dbc-pool 1.0.2",
                DatabasePerformanceReport.Status.PASSED, 4, 0, 0, List.of(scenario));
        return new DatabasePerformanceReport(
                1, "run-1", "abcdef1", "2026-08-02T00:00:00Z", "2026-08-02T00:01:00Z",
                DatabasePerformanceReport.Status.PASSED,
                new DatabasePerformanceReport.Environment("21", "Oracle", "Windows", "test cpu", 20, 1024),
                new DatabasePerformanceReport.Parameters(1, 2, 4, 4, 1, 32, 8, 4, 1000),
                List.of(database));
    }

    private static DatabasePerformanceReport.Latency latency(long samples,
                                                             double p50,
                                                             double p95,
                                                             double p99,
                                                             double max) {
        return new DatabasePerformanceReport.Latency(samples, p50, p95, p99, max);
    }
}
