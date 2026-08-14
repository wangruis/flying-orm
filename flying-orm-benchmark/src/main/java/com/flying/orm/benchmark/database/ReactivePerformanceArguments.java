package com.flying.orm.benchmark.database;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 解析并校验响应式真实库性能入口的固定参数。 */
final class ReactivePerformanceArguments {

    private static final String MYSQL_URL_ENV = "FLYING_ORM_PERFORMANCE_MYSQL_URL";
    private static final String MYSQL_DIAGNOSTICS_URL_ENV = "FLYING_ORM_PERFORMANCE_MYSQL_DIAGNOSTICS_URL";
    private static final String POSTGRESQL_URL_ENV = "FLYING_ORM_PERFORMANCE_POSTGRESQL_URL";

    String mysqlUrl;
    String mysqlDiagnosticsUrl;
    String postgresqlUrl;
    Path output;
    Path summary;
    String gitCommit;
    String runId;
    int warmupSeconds = 5;
    int measurementSeconds = 15;
    int poolSize = 16;
    int queryConcurrency = 16;
    int batchConcurrency = 4;
    int batchSize = 32;
    int independentChunkSize = 8;
    int independentConcurrency = 4;
    int seedRows = 10_000;
    boolean phaseDiagnostics;
    List<ReactivePerformanceScenario> scenarios = List.of(ReactivePerformanceScenario.values());

    static ReactivePerformanceArguments parse(String[] args) {
        String[] safeArgs = Objects.requireNonNull(args, "database performance args must not be null");
        ReactivePerformanceArguments parsed = new ReactivePerformanceArguments();
        Map<String, String> values = pairs(safeArgs);
        // 标准认证脚本使用环境变量，凭据不会出现在进程命令行；显式参数只用于本机单独调试。
        parsed.mysqlUrl = firstText(values.remove("--mysql-url"), System.getenv(MYSQL_URL_ENV));
        parsed.mysqlDiagnosticsUrl = firstText(values.remove("--mysql-diagnostics-url"),
                                               System.getenv(MYSQL_DIAGNOSTICS_URL_ENV));
        parsed.postgresqlUrl = firstText(values.remove("--postgresql-url"),
                                         System.getenv(POSTGRESQL_URL_ENV));
        parsed.output = Path.of(required(values.remove("--output"), "--output"));
        parsed.summary = Path.of(required(values.remove("--summary"), "--summary"));
        parsed.gitCommit = required(values.remove("--git-commit"), "--git-commit");
        parsed.runId = values.remove("--run-id");
        parsed.warmupSeconds = positive(values.remove("--warmup-seconds"), parsed.warmupSeconds,
                                        "--warmup-seconds", true);
        parsed.measurementSeconds = positive(values.remove("--measurement-seconds"),
                                             parsed.measurementSeconds, "--measurement-seconds", false);
        parsed.poolSize = positive(values.remove("--pool-size"), parsed.poolSize, "--pool-size", false);
        parsed.queryConcurrency = positive(values.remove("--query-concurrency"),
                                           parsed.queryConcurrency, "--query-concurrency", false);
        parsed.batchConcurrency = positive(values.remove("--batch-concurrency"),
                                           parsed.batchConcurrency, "--batch-concurrency", false);
        parsed.batchSize = positive(values.remove("--batch-size"), parsed.batchSize, "--batch-size", false);
        parsed.independentChunkSize = positive(values.remove("--independent-chunk-size"),
                                               parsed.independentChunkSize,
                                               "--independent-chunk-size", false);
        parsed.independentConcurrency = positive(values.remove("--independent-concurrency"),
                                                 parsed.independentConcurrency,
                                                 "--independent-concurrency", false);
        parsed.seedRows = positive(values.remove("--seed-rows"), parsed.seedRows, "--seed-rows", false);
        parsed.phaseDiagnostics = bool(values.remove("--phase-diagnostics"), false,
                                       "--phase-diagnostics");
        parsed.scenarios = parseScenarios(values.remove("--scenarios"));
        if (!values.isEmpty()) {
            throw new IllegalArgumentException("unknown database performance option: "
                                                       + values.keySet().iterator().next());
        }
        if (blank(parsed.mysqlUrl) && blank(parsed.postgresqlUrl)) {
            throw new IllegalArgumentException("at least one database performance URL must be configured");
        }
        parsed.reportParameters();
        return parsed;
    }

    List<ReactivePerformanceTarget> targets() {
        List<ReactivePerformanceTarget> targets = new ArrayList<>(2);
        if (!blank(mysqlUrl)) {
            targets.add(new ReactivePerformanceTarget(
                    "mysql", "MySQL", mysqlUrl, "`FLYING_ORM_PERFORMANCE_MYSQL`",
                    "drop table if exists `FLYING_ORM_PERFORMANCE_MYSQL`", "?"));
        }
        if (!blank(postgresqlUrl)) {
            targets.add(new ReactivePerformanceTarget(
                    "postgresql", "PostgreSQL", postgresqlUrl, "\"FLYING_ORM_PERFORMANCE_PG\"",
                    "drop table if exists \"FLYING_ORM_PERFORMANCE_PG\"", "$1"));
        }
        return targets;
    }

    DatabasePerformanceReport.Parameters reportParameters() {
        return new DatabasePerformanceReport.Parameters(
                warmupSeconds, measurementSeconds, poolSize, queryConcurrency, batchConcurrency,
                batchSize, independentChunkSize, independentConcurrency, seedRows);
    }

    List<String> scenarioNames() {
        return scenarios.stream().map(scenario -> scenario.externalName).toList();
    }

    boolean mysqlDiagnosticsConfigured() {
        return !blank(mysqlDiagnosticsUrl);
    }

    boolean phaseDiagnostics() {
        return phaseDiagnostics;
    }

    boolean includes(ReactivePerformanceScenario scenario) {
        return scenarios.contains(scenario);
    }

    private static Map<String, String> pairs(String[] args) {
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            if (index + 1 >= args.length || !args[index].startsWith("--") || args[index + 1].isBlank()) {
                throw new IllegalArgumentException("database performance options require --name value pairs");
            }
            String previous = values.putIfAbsent(args[index], args[index + 1]);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate database performance option: " + args[index]);
            }
        }
        return values;
    }

    private static List<ReactivePerformanceScenario> parseScenarios(String value) {
        if (blank(value)) {
            return List.of(ReactivePerformanceScenario.values());
        }
        LinkedHashSet<ReactivePerformanceScenario> selected = new LinkedHashSet<>();
        for (String name : value.split(",", -1)) {
            String normalized = name.trim();
            if (normalized.isEmpty()) {
                throw new IllegalArgumentException("database performance scenarios must not contain blank names");
            }
            if (!selected.add(ReactivePerformanceScenario.fromExternalName(normalized))) {
                throw new IllegalArgumentException("duplicate database performance scenario: " + normalized);
            }
        }
        return List.copyOf(selected);
    }

    private static int positive(String value, int fallback, String name, boolean zeroAllowed) {
        if (value == null) {
            return fallback;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(name + " requires an integer", error);
        }
        if (parsed < 0 || (!zeroAllowed && parsed == 0)) {
            throw new IllegalArgumentException(name + (zeroAllowed ? " must not be negative" : " must be positive"));
        }
        return parsed;
    }

    private static boolean bool(String value, boolean fallback, String name) {
        if (value == null) {
            return fallback;
        }
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException(name + " requires true or false");
    }

    private static String required(String value, String name) {
        if (blank(value)) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.trim();
    }

    private static String firstText(String preferred, String fallback) {
        return blank(preferred) ? fallback : preferred;
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
