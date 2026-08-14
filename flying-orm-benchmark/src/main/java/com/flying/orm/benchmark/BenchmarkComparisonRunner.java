package com.flying.orm.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/**
 * 比较两份 JMH JSON，并生成方便归档的 Markdown 报告。
 * 两边用相同的 benchmark 方法名表示同一个场景，类名和包名可以不同。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class BenchmarkComparisonRunner {

    private static final ObjectMapper JSON = new ObjectMapper();

    private BenchmarkComparisonRunner() {
    }

    /**
     * 参数格式为 {@code --flying flying.json --baseline baseline.json --output comparison.md}。
     *
     * @param args flying、baseline 和 output 文件
     * @throws IOException 文件读取或报告写入失败
     */
    public static void main(String[] args) throws IOException {
        Arguments arguments = Arguments.parse(args);
        compare(arguments.flying(), arguments.baseline(), arguments.output());
    }

    static void compare(Path flyingFile, Path baselineFile, Path outputFile) throws IOException {
        Map<String, BenchmarkResult> flying = read(flyingFile);
        Map<String, BenchmarkResult> baseline = read(baselineFile);
        List<String> scenarios = new ArrayList<>(new TreeSet<>(flying.keySet()));
        scenarios.retainAll(baseline.keySet());
        if (scenarios.isEmpty()) {
            throw new IllegalArgumentException("benchmark files do not contain matching scenario method names");
        }

        StringBuilder report = new StringBuilder(1024);
        report.append("# flying-orm 无数据库基准对比\n\n")
              .append("- flying-orm 结果：`").append(flyingFile).append("`\n")
              .append("- 基线结果：`").append(baselineFile).append("`\n")
              .append("- 改善率：吞吐模式数值越高越好，耗时模式数值越低越好。\n\n")
              .append("| 场景 | 模式 | 单位 | 基线 | flying-orm | 改善率 |\n")
              .append("| --- | --- | --- | ---: | ---: | ---: |\n");

        boolean hasPercentiles = false;
        for (String scenario : scenarios) {
            BenchmarkResult candidate = flying.get(scenario);
            BenchmarkResult source = baseline.get(scenario);
            candidate.requireComparableTo(source, scenario);
            double improvement = candidate.improvementOver(source);
            report.append(String.format(Locale.ROOT,
                                        "| %s | %s | %s | %.3f | %.3f | %+.2f%% |%n",
                                        scenario,
                                        candidate.mode(),
                                        candidate.unit(),
                                        source.score(),
                                        candidate.score(),
                                        improvement * 100));
            hasPercentiles |= candidate.hasPercentiles() || source.hasPercentiles();
        }

        if (hasPercentiles) {
            appendPercentiles(report, scenarios, flying, baseline);
        }
        report.append("\n> 只有两边在同一机器、JDK、JVM 参数和 JMH 配置下运行，结果才可用于性能结论。\n");

        Path parent = outputFile.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(outputFile, report, StandardCharsets.UTF_8);
    }

    private static void appendPercentiles(StringBuilder report,
                                          List<String> scenarios,
                                          Map<String, BenchmarkResult> flying,
                                          Map<String, BenchmarkResult> baseline) {
        report.append("\n## 延迟分位值\n\n")
              .append("| 场景 | 基线 P50 | flying P50 | 基线 P95 | flying P95 | 基线 P99 | flying P99 |\n")
              .append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
        for (String scenario : scenarios) {
            BenchmarkResult candidate = flying.get(scenario);
            BenchmarkResult source = baseline.get(scenario);
            report.append("| ").append(scenario)
                  .append(" | ").append(format(source.p50()))
                  .append(" | ").append(format(candidate.p50()))
                  .append(" | ").append(format(source.p95()))
                  .append(" | ").append(format(candidate.p95()))
                  .append(" | ").append(format(source.p99()))
                  .append(" | ").append(format(candidate.p99()))
                  .append(" |\n");
        }
    }

    private static Map<String, BenchmarkResult> read(Path file) throws IOException {
        JsonNode root = JSON.readTree(Objects.requireNonNull(file, "benchmark file must not be null").toFile());
        if (!root.isArray()) {
            throw new IllegalArgumentException("JMH result must be a JSON array: " + file);
        }
        Map<String, BenchmarkResult> results = new LinkedHashMap<>();
        for (JsonNode node : root) {
            BenchmarkResult result = BenchmarkResult.from(node);
            BenchmarkResult previous = results.putIfAbsent(result.scenario(), result);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate benchmark scenario: " + result.scenario());
            }
        }
        return results;
    }

    private static String format(Double value) {
        return value == null ? "-" : String.format(Locale.ROOT, "%.3f", value);
    }

    private record BenchmarkResult(String benchmark,
                                   String mode,
                                   int threads,
                                   int forks,
                                   String jdkVersion,
                                   int warmupIterations,
                                   String warmupTime,
                                   int measurementIterations,
                                   String measurementTime,
                                   double score,
                                   String unit,
                                   Double p50,
                                   Double p95,
                                   Double p99) {

        private static BenchmarkResult from(JsonNode node) {
            JsonNode metric = required(node, "primaryMetric");
            JsonNode percentiles = metric.path("scorePercentiles");
            return new BenchmarkResult(text(node, "benchmark"),
                                       text(node, "mode"),
                                       integer(node, "threads"),
                                       integer(node, "forks"),
                                       text(node, "jdkVersion"),
                                       integer(node, "warmupIterations"),
                                       text(node, "warmupTime"),
                                       integer(node, "measurementIterations"),
                                       text(node, "measurementTime"),
                                       number(metric, "score"),
                                       text(metric, "scoreUnit"),
                                       percentile(percentiles, "50.0"),
                                       percentile(percentiles, "95.0"),
                                       percentile(percentiles, "99.0"));
        }

        private String scenario() {
            int separator = benchmark.lastIndexOf('.');
            return separator < 0 ? benchmark : benchmark.substring(separator + 1);
        }

        private boolean hasPercentiles() {
            return p50 != null || p95 != null || p99 != null;
        }

        private double improvementOver(BenchmarkResult baseline) {
            if (baseline.score == 0) {
                throw new IllegalArgumentException("baseline score must not be zero: " + scenario());
            }
            return "thrpt".equals(mode)
                    ? (score - baseline.score) / baseline.score
                    : (baseline.score - score) / baseline.score;
        }

        private void requireComparableTo(BenchmarkResult baseline, String scenario) {
            requireSame(mode, baseline.mode, "mode", scenario);
            requireSame(unit, baseline.unit, "score unit", scenario);
            requireSame(threads, baseline.threads, "threads", scenario);
            requireSame(forks, baseline.forks, "forks", scenario);
            requireSame(jdkVersion, baseline.jdkVersion, "JDK version", scenario);
            requireSame(warmupIterations, baseline.warmupIterations, "warmup iterations", scenario);
            requireSame(warmupTime, baseline.warmupTime, "warmup time", scenario);
            requireSame(measurementIterations,
                        baseline.measurementIterations,
                        "measurement iterations",
                        scenario);
            requireSame(measurementTime, baseline.measurementTime, "measurement time", scenario);
        }

        private static void requireSame(Object left, Object right, String name, String scenario) {
            if (!Objects.equals(left, right)) {
                throw new IllegalArgumentException(
                        "benchmark " + name + " differs for scenario " + scenario + ": " + left + " != " + right);
            }
        }

        private static JsonNode required(JsonNode node, String name) {
            JsonNode value = node.get(name);
            if (value == null || value.isNull()) {
                throw new IllegalArgumentException("JMH result is missing " + name);
            }
            return value;
        }

        private static String text(JsonNode node, String name) {
            JsonNode value = required(node, name);
            if (!value.isTextual() || value.textValue().isBlank()) {
                throw new IllegalArgumentException("JMH result " + name + " must be text");
            }
            return value.textValue();
        }

        private static int integer(JsonNode node, String name) {
            JsonNode value = required(node, name);
            if (!value.canConvertToInt()) {
                throw new IllegalArgumentException("JMH result " + name + " must be an integer");
            }
            return value.intValue();
        }

        private static double number(JsonNode node, String name) {
            JsonNode value = required(node, name);
            if (!value.isNumber()) {
                throw new IllegalArgumentException("JMH result " + name + " must be a number");
            }
            return value.doubleValue();
        }

        private static Double percentile(JsonNode node, String name) {
            JsonNode value = node.get(name);
            return value == null || !value.isNumber() ? null : value.doubleValue();
        }
    }

    private record Arguments(Path flying, Path baseline, Path output) {

        private static Arguments parse(String[] args) {
            Objects.requireNonNull(args, "comparison runner args must not be null");
            Path flying = null;
            Path baseline = null;
            Path output = null;
            for (int i = 0; i < args.length; i++) {
                String option = args[i];
                Path value = Path.of(value(args, ++i, option));
                switch (option) {
                    case "--flying" -> flying = value;
                    case "--baseline" -> baseline = value;
                    case "--output" -> output = value;
                    default -> throw new IllegalArgumentException("unknown comparison option: " + option);
                }
            }
            if (flying == null || baseline == null || output == null) {
                throw new IllegalArgumentException("--flying, --baseline and --output are required");
            }
            return new Arguments(flying, baseline, output);
        }

        private static String value(String[] args, int index, String option) {
            if (index >= args.length || args[index].isBlank()) {
                throw new IllegalArgumentException(option + " requires a value");
            }
            return args[index];
        }
    }
}
