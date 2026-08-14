package com.flying.orm.benchmark;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 对比器测试使用很小的 JMH JSON，不真的执行基准。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
class BenchmarkComparisonRunnerTest {

    @TempDir
    Path tempDir;

    @Test
    void comparesMatchingScenarioAndUsesCorrectImprovementDirection() throws Exception {
        Path flying = write("flying.json", result("flying.RenderBenchmark.renderWhere", "avgt", 80.0, "ns/op"));
        Path baseline = write("baseline.json", result("baseline.RenderBenchmark.renderWhere", "avgt", 100.0, "ns/op"));
        Path report = tempDir.resolve("comparison.md");

        BenchmarkComparisonRunner.compare(flying, baseline, report);

        String markdown = Files.readString(report);
        assertTrue(markdown.contains("| renderWhere | avgt | ns/op | 100.000 | 80.000 | +20.00% |"));
        assertTrue(markdown.contains("耗时模式数值越低越好"));
    }

    @Test
    void rejectsDifferentBenchmarkConfiguration() throws Exception {
        Path flying = write("flying.json", result("flying.RenderBenchmark.renderWhere", "sample", 80.0, "ns/op"));
        Path baseline = write("baseline.json", result("baseline.RenderBenchmark.renderWhere", "sample", 100.0, "us/op"));

        assertThrows(IllegalArgumentException.class,
                     () -> BenchmarkComparisonRunner.compare(flying,
                                                               baseline,
                                                               tempDir.resolve("comparison.md")));
    }

    @Test
    void treatsHigherThroughputAsImprovement() throws Exception {
        Path flying = write("flying-throughput.json",
                            result("flying.ConditionBenchmark.compileConditions", "thrpt", 120.0, "ops/s"));
        Path baseline = write("baseline-throughput.json",
                              result("baseline.ConditionBenchmark.compileConditions", "thrpt", 100.0, "ops/s"));
        Path report = tempDir.resolve("throughput-comparison.md");

        BenchmarkComparisonRunner.compare(flying, baseline, report);

        assertTrue(Files.readString(report)
                        .contains("| compileConditions | thrpt | ops/s | 100.000 | 120.000 | +20.00% |"));
    }

    private Path write(String name, String content) throws Exception {
        Path file = tempDir.resolve(name);
        Files.writeString(file, content);
        return file;
    }

    private static String result(String benchmark, String mode, double score, String unit) {
        return """
                [{
                  "benchmark": "%s",
                  "mode": "%s",
                  "threads": 1,
                  "forks": 1,
                  "jdkVersion": "21.0.10",
                  "warmupIterations": 3,
                  "warmupTime": "1 s",
                  "measurementIterations": 5,
                  "measurementTime": "1 s",
                  "primaryMetric": {
                    "score": %s,
                    "scorePercentiles": {"50.0": %s, "95.0": %s, "99.0": %s},
                    "scoreUnit": "%s"
                  }
                }]
                """.formatted(benchmark, mode, score, score, score, score, unit);
    }
}
