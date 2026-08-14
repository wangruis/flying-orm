package com.flying.orm.benchmark.database;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 一次真实数据库性能运行的脱敏结果模型。
 *
 * <p>模型故意没有 URL、用户名、密码、SQL 文本和参数字段。Runner 只要按这个模型输出，就不会因为调试方便
 * 顺手把本机凭据带进 JSON。</p>
 *
 * @author wangr
 * @date 2026-08-02
 * @version v1.0
 */
public record DatabasePerformanceReport(int schemaVersion,
                                        String runId,
                                        String gitCommit,
                                        String startedAt,
                                        String completedAt,
                                        Status status,
                                        Environment environment,
                                        Parameters parameters,
                                        List<DatabaseResult> databases) {

    public DatabasePerformanceReport {
        if (schemaVersion != 1) {
            throw new IllegalArgumentException("database performance schema version must be 1");
        }
        runId = requireText(runId, "database performance run id");
        gitCommit = requireText(gitCommit, "database performance git commit");
        startedAt = requireText(startedAt, "database performance start time");
        completedAt = requireText(completedAt, "database performance completion time");
        status = Objects.requireNonNull(status, "database performance status must not be null");
        environment = Objects.requireNonNull(environment, "database performance environment must not be null");
        parameters = Objects.requireNonNull(parameters, "database performance parameters must not be null");
        databases = List.copyOf(Objects.requireNonNull(databases,
                                                       "database performance results must not be null"));
        if (databases.isEmpty()) {
            throw new IllegalArgumentException("database performance results must not be empty");
        }
        boolean hasFailedDatabase = databases.stream().anyMatch(result -> result.status() == Status.FAILED);
        if ((status == Status.PASSED) == hasFailedDatabase) {
            throw new IllegalArgumentException("database performance overall status conflicts with database results");
        }
    }

    /** 运行环境只记录公开信息，不读取连接配置。 */
    public record Environment(String javaVersion,
                              String javaVendor,
                              String os,
                              String cpu,
                              int logicalProcessors,
                              long maxHeapBytes) {

        public Environment {
            javaVersion = requireText(javaVersion, "database performance Java version");
            javaVendor = requireText(javaVendor, "database performance Java vendor");
            os = requireText(os, "database performance operating system");
            cpu = requireText(cpu, "database performance CPU");
            if (logicalProcessors <= 0 || maxHeapBytes <= 0) {
                throw new IllegalArgumentException("database performance processor and heap limits must be positive");
            }
        }
    }

    /** 所有会影响结果的公开参数都集中放在这里，复跑时不用猜。 */
    public record Parameters(long warmupSeconds,
                             long measurementSeconds,
                             int poolSize,
                             int queryConcurrency,
                             int batchConcurrency,
                             int batchSize,
                             int independentChunkSize,
                             int independentConcurrency,
                             int seedRows) {

        public Parameters {
            if (warmupSeconds < 0 || measurementSeconds <= 0 || poolSize <= 0 || queryConcurrency <= 0
                    || batchConcurrency <= 0 || batchSize <= 0 || independentChunkSize <= 0
                    || independentConcurrency <= 0 || seedRows <= 0) {
                throw new IllegalArgumentException("database performance parameters are outside their safe range");
            }
            if (queryConcurrency > poolSize || batchConcurrency > poolSize) {
                throw new IllegalArgumentException("database performance outer concurrency must not exceed pool size");
            }
            if (independentChunkSize > batchSize || batchSize % independentChunkSize != 0) {
                throw new IllegalArgumentException("independent chunk size must divide batch size");
            }
            if ((long) batchConcurrency * independentConcurrency > poolSize) {
                throw new IllegalArgumentException("independent batch concurrency must fit inside the connection pool");
            }
        }
    }

    /** 一个数据库包含四个场景和场景全部结束后的池状态。 */
    public record DatabaseResult(String database,
                                 String databaseVersion,
                                 String driver,
                                 String poolVersion,
                                 Status status,
                                 int finalAllocatedConnections,
                                 int finalAcquiredConnections,
                                 int finalPendingAcquires,
                                 int finalIdleConnections,
                                 List<ScenarioResult> scenarios) {

        /**
         * 保留旧 R2DBC runner 的构造方式。旧 runner 没有单独采集 idle，按 allocated-active 补出一个兼容值。
         */
        public DatabaseResult(String database,
                              String databaseVersion,
                              String driver,
                              String poolVersion,
                              Status status,
                              int finalAllocatedConnections,
                              int finalAcquiredConnections,
                              int finalPendingAcquires,
                              List<ScenarioResult> scenarios) {
            this(database, databaseVersion, driver, poolVersion, status,
                 finalAllocatedConnections, finalAcquiredConnections, finalPendingAcquires,
                 Math.max(0, finalAllocatedConnections - finalAcquiredConnections), scenarios);
        }

        public DatabaseResult {
            database = requireText(database, "database performance database name");
            databaseVersion = requireText(databaseVersion, "database performance database version");
            driver = requireText(driver, "database performance driver");
            poolVersion = requireText(poolVersion, "database performance pool version");
            status = Objects.requireNonNull(status, "database performance database status must not be null");
            scenarios = List.copyOf(Objects.requireNonNull(scenarios,
                                                           "database performance scenarios must not be null"));
            if (finalAllocatedConnections < 0 || finalAcquiredConnections < 0 || finalPendingAcquires < 0
                    || finalIdleConnections < 0) {
                throw new IllegalArgumentException("database performance final pool counters must not be negative");
            }
            if (scenarios.isEmpty()) {
                throw new IllegalArgumentException("database performance scenarios must not be empty");
            }
            boolean hasFailedScenario = scenarios.stream().anyMatch(result -> result.status() == Status.FAILED);
            boolean poolNotReturned = finalAcquiredConnections != 0 || finalPendingAcquires != 0;
            if ((status == Status.PASSED) == (hasFailedScenario || poolNotReturned)) {
                throw new IllegalArgumentException("database performance database status conflicts with scenarios or pool");
            }
        }
    }

    /** 单个场景同时记录操作吞吐、业务行吞吐、延迟和资源峰值。 */
    public record ScenarioResult(String scenario,
                                 Status status,
                                 int concurrency,
                                 long operations,
                                 long succeeded,
                                 long failed,
                                 long rows,
                                 long warmupFailures,
                                 long elapsedMillis,
                                 double operationsPerSecond,
                                 double rowsPerSecond,
                                 double errorRate,
                                 double p50Millis,
                                 double p95Millis,
                                 double p99Millis,
                                 double maxMillis,
                                 Map<String, Long> failuresByType,
                                 int peakAllocatedConnections,
                                 int peakAcquiredConnections,
                                 int peakPendingAcquires,
                                 double averageProcessCpuPercent,
                                 long peakHeapBytes,
                                 PhaseLatency phaseLatency) {

        public ScenarioResult {
            scenario = requireText(scenario, "database performance scenario");
            status = Objects.requireNonNull(status, "database performance scenario status must not be null");
            failuresByType = Map.copyOf(Objects.requireNonNull(failuresByType,
                                                               "database performance failures must not be null"));
            if (concurrency <= 0 || operations < 0 || succeeded < 0 || failed < 0 || rows < 0
                    || warmupFailures < 0 || elapsedMillis <= 0 || peakAllocatedConnections < 0
                    || peakAcquiredConnections < 0 || peakPendingAcquires < 0 || peakHeapBytes < 0) {
                throw new IllegalArgumentException("database performance scenario counters are outside their safe range");
            }
            if (operations != succeeded + failed) {
                throw new IllegalArgumentException("scenario operations must equal succeeded plus failed");
            }
            requireFiniteNonNegative(operationsPerSecond, "operations per second");
            requireFiniteNonNegative(rowsPerSecond, "rows per second");
            requireFiniteNonNegative(errorRate, "error rate");
            requireFiniteNonNegative(p50Millis, "P50 latency");
            requireFiniteNonNegative(p95Millis, "P95 latency");
            requireFiniteNonNegative(p99Millis, "P99 latency");
            requireFiniteNonNegative(maxMillis, "maximum latency");
            requireFiniteNonNegative(averageProcessCpuPercent, "process CPU");
            if (errorRate > 1.0 || p50Millis > p95Millis || p95Millis > p99Millis || p99Millis > maxMillis) {
                throw new IllegalArgumentException("database performance rates or percentiles are inconsistent");
            }
            boolean hasFailure = failed > 0 || warmupFailures > 0;
            if (status == Status.PASSED && hasFailure) {
                throw new IllegalArgumentException("passed database performance scenario must not contain failures");
            }
        }
    }

    /** 一组操作在连接获取、数据库执行及提交、连接归还和总耗时上的延迟分布。 */
    public record PhaseLatency(Latency acquire,
                               Latency executeAndCommit,
                               Latency release,
                               Latency total) {

        public PhaseLatency {
            acquire = Objects.requireNonNull(acquire, "acquire latency must not be null");
            executeAndCommit = Objects.requireNonNull(executeAndCommit,
                                                      "execute and commit latency must not be null");
            release = Objects.requireNonNull(release, "release latency must not be null");
            total = Objects.requireNonNull(total, "total latency must not be null");
        }
    }

    /** 延迟使用毫秒输出，样本数单独保留，避免把没有样本的全零结果误认为真的零耗时。 */
    public record Latency(long samples,
                          double p50Millis,
                          double p95Millis,
                          double p99Millis,
                          double maxMillis) {

        public Latency {
            if (samples < 0) {
                throw new IllegalArgumentException("latency samples must not be negative");
            }
            requireFiniteNonNegative(p50Millis, "phase P50 latency");
            requireFiniteNonNegative(p95Millis, "phase P95 latency");
            requireFiniteNonNegative(p99Millis, "phase P99 latency");
            requireFiniteNonNegative(maxMillis, "phase maximum latency");
            if (p50Millis > p95Millis || p95Millis > p99Millis || p99Millis > maxMillis) {
                throw new IllegalArgumentException("database performance phase percentiles are inconsistent");
            }
            if (samples == 0 && (p50Millis != 0 || p95Millis != 0 || p99Millis != 0 || maxMillis != 0)) {
                throw new IllegalArgumentException("latency without samples must contain zero values");
            }
        }
    }

    public enum Status {
        PASSED,
        FAILED
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    private static void requireFiniteNonNegative(double value, String name) {
        if (!Double.isFinite(value) || value < 0) {
            throw new IllegalArgumentException("database performance " + name + " must be finite and non-negative");
        }
    }
}
