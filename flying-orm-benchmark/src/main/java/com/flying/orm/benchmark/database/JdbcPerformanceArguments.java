package com.flying.orm.benchmark.database;

import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** JDBC 真实性能 runner 的参数。凭据只从环境变量进入，不会进入报告对象。 */
final class JdbcPerformanceArguments {

    static final String MYSQL_URL_ENV = "FLYING_ORM_PERFORMANCE_MYSQL_JDBC_URL";
    static final String MYSQL_USER_ENV = "FLYING_ORM_PERFORMANCE_MYSQL_JDBC_USER";
    static final String MYSQL_PASSWORD_ENV = "FLYING_ORM_PERFORMANCE_MYSQL_JDBC_PASSWORD";
    static final String POSTGRESQL_URL_ENV = "FLYING_ORM_PERFORMANCE_POSTGRESQL_JDBC_URL";
    static final String POSTGRESQL_USER_ENV = "FLYING_ORM_PERFORMANCE_POSTGRESQL_JDBC_USER";
    static final String POSTGRESQL_PASSWORD_ENV = "FLYING_ORM_PERFORMANCE_POSTGRESQL_JDBC_PASSWORD";
    static final String ORACLE_URL_ENV = "FLYING_ORM_PERFORMANCE_ORACLE_JDBC_URL";
    static final String ORACLE_USER_ENV = "FLYING_ORM_PERFORMANCE_ORACLE_JDBC_USER";
    static final String ORACLE_PASSWORD_ENV = "FLYING_ORM_PERFORMANCE_ORACLE_JDBC_PASSWORD";
    static final String SQLSERVER_URL_ENV = "FLYING_ORM_PERFORMANCE_SQLSERVER_JDBC_URL";
    static final String SQLSERVER_USER_ENV = "FLYING_ORM_PERFORMANCE_SQLSERVER_JDBC_USER";
    static final String SQLSERVER_PASSWORD_ENV = "FLYING_ORM_PERFORMANCE_SQLSERVER_JDBC_PASSWORD";

    final String mysqlUrl;
    final String mysqlUser;
    final String mysqlPassword;
    final String postgresqlUrl;
    final String postgresqlUser;
    final String postgresqlPassword;
    final String oracleUrl;
    final String oracleUser;
    final String oraclePassword;
    final String sqlserverUrl;
    final String sqlserverUser;
    final String sqlserverPassword;
    final Path output;
    final Path summary;
    final String gitCommit;
    final String runId;
    final int warmupSeconds;
    final int measurementSeconds;
    final int poolSize;
    final int queryConcurrency;
    final int batchConcurrency;
    final int batchSize;
    final int independentChunkSize;
    final int seedRows;
    final List<JdbcPerformanceScenario> scenarios;

    private JdbcPerformanceArguments(Map<String, String> environment,
                                     Path output,
                                     Path summary,
                                     String gitCommit,
                                     String runId,
                                     int warmupSeconds,
                                     int measurementSeconds,
                                     int poolSize,
                                     int queryConcurrency,
                                     int batchConcurrency,
                                     int batchSize,
                                     int independentChunkSize,
                                     int seedRows,
                                     List<JdbcPerformanceScenario> scenarios) {
        this.mysqlUrl = text(environment.get(MYSQL_URL_ENV));
        this.mysqlUser = text(environment.get(MYSQL_USER_ENV));
        this.mysqlPassword = text(environment.get(MYSQL_PASSWORD_ENV));
        this.postgresqlUrl = text(environment.get(POSTGRESQL_URL_ENV));
        this.postgresqlUser = text(environment.get(POSTGRESQL_USER_ENV));
        this.postgresqlPassword = text(environment.get(POSTGRESQL_PASSWORD_ENV));
        this.oracleUrl = text(environment.get(ORACLE_URL_ENV));
        this.oracleUser = text(environment.get(ORACLE_USER_ENV));
        this.oraclePassword = text(environment.get(ORACLE_PASSWORD_ENV));
        this.sqlserverUrl = text(environment.get(SQLSERVER_URL_ENV));
        this.sqlserverUser = text(environment.get(SQLSERVER_USER_ENV));
        this.sqlserverPassword = text(environment.get(SQLSERVER_PASSWORD_ENV));
        this.output = output;
        this.summary = summary;
        this.gitCommit = gitCommit;
        this.runId = runId;
        this.warmupSeconds = warmupSeconds;
        this.measurementSeconds = measurementSeconds;
        this.poolSize = poolSize;
        this.queryConcurrency = queryConcurrency;
        this.batchConcurrency = batchConcurrency;
        this.batchSize = batchSize;
        this.independentChunkSize = independentChunkSize;
        this.seedRows = seedRows;
        this.scenarios = List.copyOf(scenarios);
        validate();
    }

    static JdbcPerformanceArguments parse(String[] args) {
        return parse(args, System.getenv());
    }

    /** 单元测试可以传入隔离的环境变量快照，不需要碰真实数据库。 */
    static JdbcPerformanceArguments parse(String[] args, Map<String, String> environment) {
        Objects.requireNonNull(args, "jdbc benchmark args must not be null");
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            if (index + 1 >= args.length || !args[index].startsWith("--") || args[index + 1].isBlank()) {
                throw new IllegalArgumentException("JDBC benchmark options require --name value pairs");
            }
            if (values.putIfAbsent(args[index], args[index + 1]) != null) {
                throw new IllegalArgumentException("duplicate JDBC benchmark option: " + args[index]);
            }
        }
        Map<String, String> safeEnvironment = Map.copyOf(Objects.requireNonNull(environment,
                                                                             "benchmark environment must not be null"));
        Path output = Path.of(required(values.remove("--output"), "--output"));
        Path summary = Path.of(required(values.remove("--summary"), "--summary"));
        String gitCommit = required(values.remove("--git-commit"), "--git-commit");
        String runId = values.remove("--run-id");
        if (blank(runId)) {
            runId = "jdbc-" + Instant.now().toString().replace(":", "").replace(".", "");
        }
        int warmup = positive(values.remove("--warmup-seconds"), 5, "--warmup-seconds", true);
        int measurement = positive(values.remove("--measurement-seconds"), 15, "--measurement-seconds", false);
        int pool = positive(values.remove("--pool-size"), 16, "--pool-size", false);
        int query = positive(values.remove("--query-concurrency"), pool, "--query-concurrency", false);
        String batchConcurrencyText = values.remove("--batch-concurrency");
        int batchConcurrency = batchConcurrencyText == null
                ? Math.min(4, pool)
                : positive(batchConcurrencyText, 1, "--batch-concurrency", false);
        int batchSize = positive(values.remove("--batch-size"), 32, "--batch-size", false);
        int independentChunkSize = positive(values.remove("--independent-chunk-size"),
                                            defaultChunkSize(batchSize), "--independent-chunk-size", false);
        int seedRows = positive(values.remove("--seed-rows"), 10_000, "--seed-rows", false);
        List<JdbcPerformanceScenario> scenarios = parseScenarios(values.remove("--scenarios"));
        if (!values.isEmpty()) {
            throw new IllegalArgumentException("unknown JDBC benchmark option: " + values.keySet().iterator().next());
        }
        JdbcPerformanceArguments parsed = new JdbcPerformanceArguments(safeEnvironment, output, summary, gitCommit,
                                                                         runId, warmup, measurement, pool, query,
                                                                         batchConcurrency, batchSize, independentChunkSize,
                                                                         seedRows, scenarios);
        if (parsed.mysqlUrl.isBlank() && parsed.postgresqlUrl.isBlank()
                && parsed.oracleUrl.isBlank() && parsed.sqlserverUrl.isBlank()) {
            throw new IllegalArgumentException("at least one JDBC performance URL must be configured: "
                                                       + MYSQL_URL_ENV + ", " + POSTGRESQL_URL_ENV + ", "
                                                       + ORACLE_URL_ENV + " or " + SQLSERVER_URL_ENV);
        }
        parsed.requireCredentials();
        return parsed;
    }

    DatabasePerformanceReport.Parameters reportParameters() {
        return new DatabasePerformanceReport.Parameters(warmupSeconds, measurementSeconds, poolSize,
                                                        queryConcurrency, batchConcurrency, batchSize,
                                                        independentChunkSize, 1,
                                                        seedRows);
    }

    private void requireCredentials() {
        if (!mysqlUrl.isBlank()) {
            requireText(mysqlUser, MYSQL_USER_ENV);
            requireText(mysqlPassword, MYSQL_PASSWORD_ENV);
        }
        if (!postgresqlUrl.isBlank()) {
            requireText(postgresqlUser, POSTGRESQL_USER_ENV);
            requireText(postgresqlPassword, POSTGRESQL_PASSWORD_ENV);
        }
        if (!oracleUrl.isBlank()) {
            requireText(oracleUser, ORACLE_USER_ENV);
            requireText(oraclePassword, ORACLE_PASSWORD_ENV);
        }
        if (!sqlserverUrl.isBlank()) {
            requireText(sqlserverUser, SQLSERVER_USER_ENV);
            requireText(sqlserverPassword, SQLSERVER_PASSWORD_ENV);
        }
    }

    private void validate() {
        if (warmupSeconds < 0 || measurementSeconds <= 0 || poolSize <= 0 || queryConcurrency <= 0
                || batchConcurrency <= 0 || batchSize <= 0 || independentChunkSize <= 0 || seedRows <= 0) {
            throw new IllegalArgumentException("JDBC benchmark numeric options are outside their safe range");
        }
        if (queryConcurrency > poolSize || batchConcurrency > poolSize) {
            throw new IllegalArgumentException("JDBC benchmark concurrency must fit inside pool size");
        }
        if (independentChunkSize > batchSize || batchSize % independentChunkSize != 0) {
            throw new IllegalArgumentException("JDBC independent chunk size must divide batch size");
        }
    }

    private static List<JdbcPerformanceScenario> parseScenarios(String value) {
        if (blank(value)) {
            return List.of(JdbcPerformanceScenario.values());
        }
        LinkedHashSet<JdbcPerformanceScenario> selected = new LinkedHashSet<>();
        for (String item : value.split(",", -1)) {
            String name = item.trim();
            if (name.isEmpty()) {
                throw new IllegalArgumentException("JDBC benchmark scenarios must not contain blank names");
            }
            JdbcPerformanceScenario scenario = JdbcPerformanceScenario.fromName(name);
            if (!selected.add(scenario)) {
                throw new IllegalArgumentException("duplicate JDBC benchmark scenario: " + name);
            }
        }
        return List.copyOf(selected);
    }

    private static int defaultChunkSize(int batchSize) {
        for (int candidate = Math.min(8, batchSize); candidate > 1; candidate--) {
            if (batchSize % candidate == 0) return candidate;
        }
        return 1;
    }

    private static int positive(String value, int fallback, String name, boolean zeroAllowed) {
        if (value == null) {
            return fallback;
        }
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0 || (!zeroAllowed && parsed == 0)) {
                throw new IllegalArgumentException(name + (zeroAllowed ? " must not be negative" : " must be positive"));
            }
            return parsed;
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(name + " requires an integer", error);
        }
    }

    private static String required(String value, String name) {
        if (blank(value)) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.trim();
    }

    private static String text(String value) {
        return value == null ? "" : value.trim();
    }

    private static void requireText(String value, String name) {
        if (blank(value)) {
            throw new IllegalArgumentException(name + " is required when its JDBC URL is configured");
        }
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
