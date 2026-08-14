package com.flying.orm.benchmark.database;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 只验证 runner 的参数和脱敏契约，不打开连接，也不依赖本地 MySQL/PostgreSQL。 */
class JdbcDatabasePerformanceRunnerTest {

    Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory(Path.of("target"), "jdbc-runner-");
    }

    @Test
    void parsesCompatibleOptionsAndJdbcEnvironment() {
        JdbcPerformanceArguments arguments = JdbcPerformanceArguments.parse(arguments(), Map.of(
                JdbcPerformanceArguments.MYSQL_URL_ENV, "jdbc:mysql://localhost:3306/bench",
                JdbcPerformanceArguments.MYSQL_USER_ENV, "bench",
                JdbcPerformanceArguments.MYSQL_PASSWORD_ENV, "secret"));

        assertEquals("jdbc:mysql://localhost:3306/bench", arguments.mysqlUrl);
        assertEquals(2, arguments.poolSize);
        assertEquals(2, arguments.queryConcurrency);
        assertEquals(2, arguments.batchSize);
        assertEquals(2, arguments.independentChunkSize);
        assertEquals(List.of(JdbcPerformanceScenario.QUERY_BY_ID), arguments.scenarios);
        assertFalse(arguments.runId.contains("secret"));
    }

    @Test
    void rejectsMissingTargetsAndIncompleteCredentialsBeforeOpeningAConnection() {
        IllegalArgumentException missingTarget = assertThrows(IllegalArgumentException.class,
                () -> JdbcPerformanceArguments.parse(arguments(), Map.of()));
        assertTrue(missingTarget.getMessage().contains("at least one JDBC performance URL"));

        IllegalArgumentException missingPassword = assertThrows(IllegalArgumentException.class,
                () -> JdbcPerformanceArguments.parse(arguments(), Map.of(
                        JdbcPerformanceArguments.POSTGRESQL_URL_ENV, "jdbc:postgresql://localhost/bench",
                        JdbcPerformanceArguments.POSTGRESQL_USER_ENV, "bench")));
        assertTrue(missingPassword.getMessage().contains(JdbcPerformanceArguments.POSTGRESQL_PASSWORD_ENV));
    }

    /** 被驱动包装的 JVM 致命错误不能降级成普通失败报告。 */
    @Test
    void propagatesNestedVirtualMachineErrorFromFailedDatabaseReport() {
        OutOfMemoryError fatal = new OutOfMemoryError("simulated fatal error");
        IllegalStateException wrapper = new IllegalStateException("driver wrapper", fatal);

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                () -> JdbcPerformanceReportSupport.failedDatabase("MySQL", wrapper));

        assertSame(fatal, observed);
    }

    @Test
    void writesPoolIdleStateWithoutConnectionSecrets() throws Exception {
        DatabasePerformanceReport.ScenarioResult scenario = new DatabasePerformanceReport.ScenarioResult(
                "queryById", DatabasePerformanceReport.Status.PASSED, 1,
                1, 1, 0, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1,
                Map.of(), 1, 1, 0, 0.0, 0L, null);
        DatabasePerformanceReport.DatabaseResult database = new DatabasePerformanceReport.DatabaseResult(
                "MySQL", "8", "MySQL JDBC", "HikariCP 7.0.2",
                DatabasePerformanceReport.Status.PASSED, 1, 0, 0, 1, List.of(scenario));
        DatabasePerformanceReport report = new DatabasePerformanceReport(
                1, "run-1", "abcdef1", "2026-08-08T00:00:00Z", "2026-08-08T00:00:01Z",
                DatabasePerformanceReport.Status.PASSED,
                new DatabasePerformanceReport.Environment("21", "OpenJDK", "Windows", "test-cpu", 2, 1024),
                new DatabasePerformanceReport.Parameters(0, 1, 1, 1, 1, 1, 1, 1, 1),
                List.of(database));
        Path json = tempDir.resolve("report.json");
        Path markdown = tempDir.resolve("report.md");

        DatabasePerformanceReportWriter.write(report, json, markdown);

        String output = Files.readString(json) + Files.readString(markdown);
        assertTrue(output.contains("finalIdleConnections"));
        assertTrue(output.contains("最终池状态：allocated=1，acquired=0，idle=1，pending=0"));
        assertFalse(output.contains("jdbc:mysql://localhost"));
        assertFalse(output.toLowerCase().contains("secret"));
    }

    private String[] arguments() {
        return new String[]{
                "--output", tempDir.resolve("report.json").toString(),
                "--summary", tempDir.resolve("summary.md").toString(),
                "--git-commit", "abcdef1",
                "--run-id", "jdbc-test",
                "--warmup-seconds", "0",
                "--measurement-seconds", "1",
                "--pool-size", "2",
                "--query-concurrency", "2",
                "--batch-concurrency", "1",
                "--batch-size", "2",
                "--scenarios", "queryById"
        };
    }
}
