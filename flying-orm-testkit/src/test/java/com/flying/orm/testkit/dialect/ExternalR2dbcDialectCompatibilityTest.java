package com.flying.orm.testkit.dialect;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.array.ArrayConditionValue;
import com.flying.orm.rdb.array.ArrayTermHandlers;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlLargeObjectLimitExceededException;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.FormSchemaSqlRenderer;
import com.flying.orm.testkit.concurrent.ReactiveConcurrencyProbe;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 显式连接真实数据库时才跑。平时没有配置 URL，就安静跳过。
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
class ExternalR2dbcDialectCompatibilityTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Test
    void runsMysqlSmokeScenarioWhenUrlIsConfigured() {
        runIfConfigured("flying.orm.compat.mysql.url",
                        RdbDialect.mysql(),
                        "FLYING_ORM_SMOKE_MYSQL",
                        "drop table if exists `FLYING_ORM_SMOKE_MYSQL`");
    }

    @Test
    void runsPostgresqlSmokeScenarioWhenUrlIsConfigured() {
        runIfConfigured("flying.orm.compat.postgresql.url",
                        RdbDialect.postgresql(),
                        "FLYING_ORM_SMOKE_PG",
                        "drop table if exists \"FLYING_ORM_SMOKE_PG\"");
    }

    @Test
    void runsOracleSmokeScenarioWhenUrlIsConfigured() {
        runIfConfigured("flying.orm.compat.oracle.url",
                        RdbDialect.oracle(),
                        "FLYING_ORM_SMOKE_ORACLE",
                        "drop table \"FLYING_ORM_SMOKE_ORACLE\"");
    }

    @Test
    void runsSqlServerSmokeScenarioWhenUrlIsConfigured() {
        runIfConfigured("flying.orm.compat.sqlserver.url",
                        RdbDialect.sqlServer(),
                        "FLYING_ORM_SMOKE_SQLSERVER",
                        "drop table if exists \"FLYING_ORM_SMOKE_SQLSERVER\"");
    }

    @Test
    void runsMysqlJsonScenarioWhenUrlIsConfigured() {
        runJsonIfConfigured("flying.orm.compat.mysql.url",
                            RdbDialect.mysql(),
                            "FLYING_ORM_JSON_MYSQL",
                            "drop table if exists `FLYING_ORM_JSON_MYSQL`");
    }

    @Test
    void runsPostgresqlJsonScenarioWhenUrlIsConfigured() {
        runJsonIfConfigured("flying.orm.compat.postgresql.url",
                            RdbDialect.postgresql(),
                            "FLYING_ORM_JSON_PG",
                            "drop table if exists \"FLYING_ORM_JSON_PG\"");
    }

    @Test
    void runsOracleJsonScenarioWhenUrlIsConfigured() {
        runJsonIfConfigured("flying.orm.compat.oracle.url",
                            RdbDialect.oracle(),
                            "FLYING_ORM_JSON_ORACLE",
                            "drop table \"FLYING_ORM_JSON_ORACLE\"");
    }

    @Test
    void runsSqlServerJsonScenarioWhenUrlIsConfigured() {
        runJsonIfConfigured("flying.orm.compat.sqlserver.url",
                            RdbDialect.sqlServer(),
                            "FLYING_ORM_JSON_SQLSERVER",
                            "drop table if exists \"FLYING_ORM_JSON_SQLSERVER\"");
    }

    @Test
    void runsPostgresqlArrayScenarioWhenUrlIsConfigured() {
        runPostgresqlArrayIfConfigured("flying.orm.compat.postgresql.url",
                                       "FLYING_ORM_ARRAY_PG",
                                       "drop table if exists \"FLYING_ORM_ARRAY_PG\"");
    }

    @Test
    void runsMysqlLargeObjectScenarioWhenUrlIsConfigured() {
        runLargeObjectIfConfigured("flying.orm.compat.mysql.url",
                                   RdbDialect.mysql(),
                                   "FLYING_ORM_LOB_MYSQL",
                                   "drop table if exists `FLYING_ORM_LOB_MYSQL`");
    }

    @Test
    void runsPostgresqlLargeObjectScenarioWhenUrlIsConfigured() {
        runLargeObjectIfConfigured("flying.orm.compat.postgresql.url",
                                   RdbDialect.postgresql(),
                                   "FLYING_ORM_LOB_PG",
                                   "drop table if exists \"FLYING_ORM_LOB_PG\"");
    }

    @Test
    void runsOracleLargeObjectScenarioWhenUrlIsConfigured() {
        runLargeObjectIfConfigured("flying.orm.compat.oracle.url",
                                   RdbDialect.oracle(),
                                   "FLYING_ORM_LOB_ORACLE",
                                   "drop table \"FLYING_ORM_LOB_ORACLE\"");
    }

    @Test
    void runsSqlServerLargeObjectScenarioWhenUrlIsConfigured() {
        runLargeObjectIfConfigured("flying.orm.compat.sqlserver.url",
                                   RdbDialect.sqlServer(),
                                   "FLYING_ORM_LOB_SQLSERVER",
                                   "drop table if exists \"FLYING_ORM_LOB_SQLSERVER\"");
    }

    @Test
    void runsMysqlOptimisticLockScenarioWhenUrlIsConfigured() {
        runOptimisticLockIfConfigured("flying.orm.compat.mysql.url",
                                     RdbDialect.mysql(),
                                     "FLYING_ORM_LOCK_MYSQL",
                                     "drop table if exists `FLYING_ORM_LOCK_MYSQL`");
    }

    @Test
    void runsPostgresqlOptimisticLockScenarioWhenUrlIsConfigured() {
        runOptimisticLockIfConfigured("flying.orm.compat.postgresql.url",
                                     RdbDialect.postgresql(),
                                     "FLYING_ORM_LOCK_PG",
                                     "drop table if exists \"FLYING_ORM_LOCK_PG\"");
    }

    @Test
    void runsOracleOptimisticLockScenarioWhenUrlIsConfigured() {
        runOptimisticLockIfConfigured("flying.orm.compat.oracle.url",
                                      RdbDialect.oracle(),
                                      "FLYING_ORM_LOCK_ORACLE",
                                      "drop table \"FLYING_ORM_LOCK_ORACLE\"");
    }

    @Test
    void runsSqlServerOptimisticLockScenarioWhenUrlIsConfigured() {
        runOptimisticLockIfConfigured("flying.orm.compat.sqlserver.url",
                                      RdbDialect.sqlServer(),
                                      "FLYING_ORM_LOCK_SQLSERVER",
                                      "drop table if exists \"FLYING_ORM_LOCK_SQLSERVER\"");
    }

    @Test
    void readsMysqlMetadataWhenUrlIsConfigured() {
        readMetadataIfConfigured("flying.orm.compat.mysql.url",
                                 RdbDialect.mysql(),
                                 "FLYING_ORM_META_MYSQL_CHILD",
                                 "drop table if exists `FLYING_ORM_META_MYSQL_CHILD`",
                                 "drop table if exists `FLYING_ORM_META_MYSQL_PARENT`");
    }

    @Test
    void readsPostgresqlMetadataWhenUrlIsConfigured() {
        readMetadataIfConfigured("flying.orm.compat.postgresql.url",
                                 RdbDialect.postgresql(),
                                 "FLYING_ORM_META_PG_CHILD",
                                 "drop table if exists \"FLYING_ORM_META_PG_CHILD\"",
                                 "drop table if exists \"FLYING_ORM_META_PG_PARENT\"");
    }

    @Test
    void readsOracleMetadataWhenUrlIsConfigured() {
        readMetadataIfConfigured("flying.orm.compat.oracle.url",
                                 RdbDialect.oracle(),
                                 "FLYING_ORM_META_ORACLE_CHILD",
                                 "drop table \"FLYING_ORM_META_ORACLE_CHILD\"",
                                 "drop table \"FLYING_ORM_META_ORACLE_PARENT\"");
    }

    @Test
    void readsSqlServerMetadataWhenUrlIsConfigured() {
        readMetadataIfConfigured("flying.orm.compat.sqlserver.url",
                                 RdbDialect.sqlServer(),
                                 "FLYING_ORM_META_SQLSERVER_CHILD",
                                 "drop table if exists \"FLYING_ORM_META_SQLSERVER_CHILD\"",
                                 "drop table if exists \"FLYING_ORM_META_SQLSERVER_PARENT\"");
    }

    /**
     * 用真实 MySQL 驱动确认有界并发下连接和查询都能正常收口。
     */
    @Test
    void runsMysqlBoundedConcurrencyWhenUrlIsConfigured() {
        runConcurrencyIfConfigured("flying.orm.compat.mysql.url");
    }

    /**
     * 用真实 PostgreSQL 驱动确认有界并发下连接和查询都能正常收口。
     */
    @Test
    void runsPostgresqlBoundedConcurrencyWhenUrlIsConfigured() {
        runConcurrencyIfConfigured("flying.orm.compat.postgresql.url");
    }

    /** Oracle 也必须在同一套有界 worker 下完成请求，不能因为驱动实现不同退回阻塞循环。 */
    @Test
    void runsOracleBoundedConcurrencyWhenUrlIsConfigured() {
        runConcurrencyIfConfigured("flying.orm.compat.oracle.url");
    }

    /** SQL Server 使用相同并发口径，确保业务换方言时不需要换执行模型。 */
    @Test
    void runsSqlServerBoundedConcurrencyWhenUrlIsConfigured() {
        runConcurrencyIfConfigured("flying.orm.compat.sqlserver.url");
    }

    private static void runConcurrencyIfConfigured(String urlProperty) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");

        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(ConnectionFactories.get(url));
        SqlExecutionOptions options = SqlExecutionOptions.timeout(Duration.ofSeconds(5))
                                                               .withConnectionAcquireTimeout(Duration.ofSeconds(2));
        ReactiveConcurrencyProbe.Plan plan = new ReactiveConcurrencyProbe.Plan(32, 8, TIMEOUT);
        ReactiveConcurrencyProbe.Result result = ReactiveConcurrencyProbe.run(
                plan,
                ignored -> executor.query(SqlRequest.nativeSql("select 1 as FLYING_VALUE", List.of()), options)
                                   .then())
                                                                               .block(TIMEOUT.plusSeconds(5));

        assertEquals(32, result.completed());
        assertEquals(0, result.failed());
        assertEquals(0, result.cancelled());
        assertFalse(result.timedOut());
        assertTrue(result.maxInFlight() <= 8);
    }

    private static void runIfConfigured(String urlProperty,
                                        RdbDialect dialect,
                                        String tableName,
                                        String cleanupSql) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");

        ConnectionFactory connectionFactory = ConnectionFactories.get(url);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        ReactiveDialectSmokeScenario scenario = ReactiveDialectSmokeScenario.forTable(tableName);

        ReactiveDialectSmokeResult result = cleanup(executor, cleanupSql)
                .then(scenario.run(executor, dialect))
                .block(TIMEOUT);

        assertEquals(1L, result.insertedRows());
        // MySQL 把“命中后更新”的一行记成 2，再加上新插入的一行，所以这里会返回 3。
        // ORM 保留数据库和驱动给出的真实影响行数，上层做审计或观测时才不会被一个统一假值误导。
        long expectedUpsertedRows = "mysql".equals(dialect.name()) ? 3L : 2L;
        assertEquals(expectedUpsertedRows, result.upsertedRows());
        assertEquals(1L, result.pageResult().total());
        assertEquals(1, result.pageResult().rows().size());
        assertEquals(1L, result.deletedRows());
        assertEquals(1, result.remainingRows().size());
    }

    /**
     * Oracle 没有通用的 DROP TABLE IF EXISTS。只忽略测试开始前的清理失败，真正的建表和 CRUD 错误不能被吞掉。
     */
    private static Mono<Long> cleanup(R2dbcSqlExecutor executor, String cleanupSql) {
        return executor.rowsUpdated(SqlRequest.nativeSql(cleanupSql, List.of()))
                       .onErrorResume(ignored -> Mono.empty());
    }

    private static void readMetadataIfConfigured(String urlProperty,
                                                 RdbDialect dialect,
                                                 String childTableName,
                                                 String cleanupChildSql,
                                                 String cleanupParentSql) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");

        ConnectionFactory connectionFactory = ConnectionFactories.get(url);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        ReactiveDialectMetadataScenario scenario = ReactiveDialectMetadataScenario.forTable(childTableName);

        ReactiveDialectMetadataResult result = cleanup(executor, cleanupChildSql)
                                                      .then(cleanup(executor, cleanupParentSql))
                                                      .then(scenario.run(executor, dialect))
                                                      .block(TIMEOUT);

        assertEquals(childTableName, result.tableName());
        assertEquals(List.of("ID"), result.primaryKeys());
        assertEquals(List.of("NAME"), result.uniqueIndexColumns());
        assertEquals(List.of("PARENT_ID"), result.foreignKeyColumns());
        assertEquals(scenario.parentTableName(), result.referencedTableName());
    }

    /**
     * 这条链路专门检查真实驱动如何绑定和读回 JSON。URL 不配置时跳过，CI 接上数据库就会自动执行。
     */
    private static void runJsonIfConfigured(String urlProperty,
                                            RdbDialect dialect,
                                            String tableName,
                                            String cleanupSql) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");

        ConnectionFactory connectionFactory = ConnectionFactories.get(url);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(conditionRenderer(), dialect));
        DynamicForm form = DynamicForm.builder("jsonSmoke", tableName)
                                      .addField(DynamicField.primaryKey("ID", "BIGINT"))
                                      .addField(DynamicField.of("PROFILE", "JSON"))
                                      .build();
        Map<String, Object> updated = row("name", "Alice", "roles", List.of("admin", "auditor"));
        Map<String, Object> second = row("name", "Bob", "roles", List.of("user"));

        List<DynamicRow> rows = cleanup(executor, cleanupSql)
                                                 .thenMany(Flux.fromIterable(FormSchemaSqlRenderer.create(dialect)
                                                                                                  .createTable(form)))
                                                 .concatMap(executor::rowsUpdated)
                                                 .then(client.insert(WriteSpec.insert(
                                                         form,
                                                         row("ID", 1L,
                                                             "PROFILE", row("name", "Alice")))))
                                                 .then(client.writeBatch(BatchSpec.upsert(
                                                         form,
                                                         Flux.fromIterable(List.of(row("ID", 1L,
                                                                                      "PROFILE", updated),
                                                                                  row("ID", 2L,
                                                                                      "PROFILE", second))))))
                                                 .thenMany(client.select(QuerySpec.of(
                                                         form, ConditionGroup.and().build())))
                                                 .collectList()
                                                 .block(TIMEOUT);

        assertEquals(2, rows.size());
        Map<Long, Object> profiles = new LinkedHashMap<>();
        for (Map<String, Object> result : rows) {
            profiles.put(((Number) value(result, "ID")).longValue(), value(result, "PROFILE"));
        }
        assertEquals(updated, profiles.get(1L));
        assertEquals(second, profiles.get(2L));
    }

    /**
     * PostgreSQL 数组必须由真实驱动绑定强类型数组。这个入口同时验证写入、批量 upsert、读回和数组条件。
     */
    private static void runPostgresqlArrayIfConfigured(String urlProperty,
                                                       String tableName,
                                                       String cleanupSql) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");

        ConnectionFactory connectionFactory = ConnectionFactories.get(url);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        RdbDialect dialect = RdbDialect.postgresql();
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .addTermPackage(ArrayTermHandlers.postgresql())
                                          .build();
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(renderer, dialect));
        DynamicForm form = DynamicForm.builder("arraySmoke", tableName)
                                      .addField(DynamicField.primaryKey("ID", "BIGINT"))
                                      .addField(DynamicField.of("TAGS", "VARCHAR[]"))
                                      .build();

        List<DynamicRow> rows = cleanup(executor, cleanupSql)
                                                 .thenMany(Flux.fromIterable(FormSchemaSqlRenderer.create(dialect)
                                                                                                  .createTable(form)))
                                                 .concatMap(executor::rowsUpdated)
                                                 .then(client.insert(WriteSpec.insert(
                                                         form,
                                                         row("ID", 1L, "TAGS", List.of("java")))))
                                                 .then(client.writeBatch(BatchSpec.upsert(
                                                         form,
                                                         Flux.fromIterable(List.of(row("ID", 1L,
                                                                                      "TAGS", List.of("java", "r2dbc")),
                                                                                   row("ID", 2L,
                                                                                       "TAGS", List.of("mysql")))))))
                                                 .thenMany(client.select(QuerySpec.of(
                                                         form,
                                                         ConditionGroup.and()
                                                                       .where("TAGS",
                                                                              "array-overlaps",
                                                                              ArrayConditionValue.of(
                                                                                       List.of("r2dbc"),
                                                                                       "VARCHAR[]"))
                                                                       .build())))
                                                 .collectList()
                                                 .block(TIMEOUT);

        assertEquals(1, rows.size());
        assertEquals(List.of("java", "r2dbc"), value(rows.getFirst(), "TAGS"));
    }

    /**
     * 用真实驱动走一遍大字段绑定和读取。这里不关心驱动返回 byte[]、ByteBuffer 还是 Blob，
     * 只要求客户端最后给业务稳定的 byte[]/String，并且同一套大小保护始终生效。
     */
    private static void runLargeObjectIfConfigured(String urlProperty,
                                                    RdbDialect dialect,
                                                    String tableName,
                                                    String cleanupSql) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");

        ConnectionFactory connectionFactory = ConnectionFactories.get(url);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(conditionRenderer(), dialect));
        DynamicForm form = DynamicForm.builder("largeObjectSmoke", tableName)
                                      .addField(DynamicField.primaryKey("ID", "BIGINT"))
                                      .addField(DynamicField.of("PAYLOAD", "BLOB"))
                                      .addField(DynamicField.of("CONTENT", "CLOB"))
                                      .build();
        byte[] initialPayload = payload(64 * 1024, (byte) 7);
        byte[] updatedPayload = payload(96 * 1024, (byte) 11);
        byte[] secondPayload = payload(32 * 1024, (byte) 19);
        String updatedContent = "updated-large-text-".repeat(4 * 1024);
        String secondContent = "second-large-text-".repeat(2 * 1024);
        List<Map<String, Object>> batchRows = List.of(row("ID", 1L,
                                                         "PAYLOAD", updatedPayload,
                                                         "CONTENT", updatedContent),
                                                      row("ID", 2L,
                                                          "PAYLOAD", secondPayload,
                                                          "CONTENT", secondContent));
        SqlExecutionOptions readProtection = SqlExecutionOptions.timeout(TIMEOUT)
                                                                      .withMaxRows(10)
                                                                      .withMaxLargeObjectBytes(updatedPayload.length)
                                                                      .withMaxLargeObjectChars(updatedContent.length());

        List<DynamicRow> rows = cleanup(executor, cleanupSql)
                                                 .thenMany(Flux.fromIterable(FormSchemaSqlRenderer.create(dialect)
                                                                                                  .createTable(form)))
                                                 .concatMap(executor::rowsUpdated)
                                                 .then(client.insert(WriteSpec.insert(
                                                         form,
                                                         row("ID", 1L,
                                                                         "PAYLOAD", initialPayload,
                                                                         "CONTENT", "initial-large-text"))))
                                                 // 先覆盖轻量 List 批量，再覆盖默认推荐的 ATOMIC 事务批量；Oracle
                                                 // 对两条路径的 LOB 绑定处理不同，真实认证不能只验证其中一条。
                                                 .then(client.writeBatch(BatchSpec.upsert(
                                                         form, Flux.fromIterable(batchRows))))
                                                 .then(client.writeBatch(BatchSpec.upsert(
                                                         form, Flux.fromIterable(batchRows))
                                                                 .withOptions(BatchWriteOptions.atomic(2))))
                                                 .doOnNext(result -> assertEquals(BatchWriteResult.Status.COMMITTED,
                                                                                  result.status()))
                                                 .thenMany(client.select(QuerySpec.of(
                                                         form, ConditionGroup.and().build())
                                                                 .withExecutionOptions(readProtection)))
                                                 .collectList()
                                                 .block(TIMEOUT);

        assertEquals(2, rows.size());
        Map<Long, Map<String, Object>> rowsById = new LinkedHashMap<>();
        for (Map<String, Object> result : rows) {
            rowsById.put(((Number) value(result, "ID")).longValue(), result);
        }
        assertArrayEquals(updatedPayload, (byte[]) value(rowsById.get(1L), "PAYLOAD"));
        assertEquals(updatedContent, value(rowsById.get(1L), "CONTENT"));
        assertArrayEquals(secondPayload, (byte[]) value(rowsById.get(2L), "PAYLOAD"));
        assertEquals(secondContent, value(rowsById.get(2L), "CONTENT"));

        SqlExecutionOptions tooSmall = readProtection.withMaxLargeObjectBytes(16);
        SqlLargeObjectLimitExceededException error = assertThrows(
                SqlLargeObjectLimitExceededException.class,
                () -> client.select(QuerySpec.of(
                                    form,
                                    ConditionGroup.and().where("ID", "=", 1L).build())
                                    .withExecutionOptions(tooSmall))
                            .collectList()
                            .block(TIMEOUT));
        assertEquals(SqlLargeObjectLimitExceededException.Kind.BINARY, error.kind());
        assertEquals(16L, error.maxSize());
    }

    /**
     * 先提交一次正常更新，再让同一事务里的第二行制造版本冲突。最终值没变化，才说明第一行真的回滚了。
     */
    private static void runOptimisticLockIfConfigured(String urlProperty,
                                                       RdbDialect dialect,
                                                       String tableName,
                                                       String cleanupSql) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");

        ConnectionFactory connectionFactory = ConnectionFactories.get(url);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        DynamicForm form = versionedForm(tableName);
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(conditionRenderer(), dialect));

        Map<String, Object> finalRow = cleanup(executor, cleanupSql)
                                               .thenMany(Flux.fromIterable(FormSchemaSqlRenderer.create(dialect)
                                                                                              .createTable(form)))
                                               .concatMap(executor::rowsUpdated)
                                               .then(client.insert(WriteSpec.insert(
                                                       form,
                                                       row("ID", "u1",
                                                                       "NAME", "Alice",
                                                                       "VERSION", 1L))))
                                               .then(client.writeBatch(BatchSpec.update(
                                                       form, Flux.just(update("Alice-2", 1L)))
                                                               .withOptions(BatchWriteOptions.atomic(1))))
                                               .doOnNext(result -> assertEquals(BatchWriteResult.Status.COMMITTED,
                                                                                result.status()))
                                               .then(client.writeBatch(BatchSpec.update(
                                                       form,
                                                       Flux.just(update("must-roll-back", 2L),
                                                                 update("stale", 99L)))
                                                               .withOptions(BatchWriteOptions.atomic(2))))
                                               .then(Mono.<BatchWriteResult>error(new AssertionError(
                                                       "stale batch should conflict")))
                                               .onErrorResume(BatchWriteException.class, error -> {
                                                   assertEquals(BatchWriteResult.Status.ROLLED_BACK,
                                                                error.result().status());
                                                   assertEquals(1L, error.result().conflictCount());
                                                   assertEquals(1L,
                                                                error.result()
                                                                     .chunks()
                                                                     .getFirst()
                                                                     .conflicts()
                                                                     .getFirst()
                                                                     .inputOffset());
                                                   return Mono.just(error.result());
                                               })
                                               .thenMany(client.select(QuerySpec.of(
                                                       form,
                                                       ConditionGroup.and()
                                                                     .where("ID", "=", "u1")
                                                                     .build())))
                                               .next()
                                               .block(TIMEOUT);

        assertEquals("Alice-2", finalRow.get("NAME"));
        assertEquals(2L, ((Number) finalRow.get("VERSION")).longValue());
    }

    private static DynamicForm versionedForm(String tableName) {
        return DynamicForm.builder("optimisticLockSmoke", tableName)
                          .addField(DynamicField.primaryKey("ID", "VARCHAR"))
                          .addField(DynamicField.of("NAME", "VARCHAR"))
                          .addField(DynamicField.of("VERSION", "BIGINT"))
                          .build();
    }

    private static BatchOptimisticUpdate update(String name, long version) {
        return new BatchOptimisticUpdate(row("NAME", name),
                                         ConditionGroup.and().where("ID", "=", "u1").build(),
                                         OptimisticLockOptions.increment("VERSION", version));
    }

    private static SqlRenderer conditionRenderer() {
        return SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build();
    }

    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            values.put((String) pairs[index], pairs[index + 1]);
        }
        return values;
    }

    private static byte[] payload(int size, byte value) {
        byte[] payload = new byte[size];
        Arrays.fill(payload, value);
        return payload;
    }

    private static Object value(Map<String, Object> row, String name) {
        return row.entrySet()
                  .stream()
                  .filter(entry -> entry.getKey().equalsIgnoreCase(name))
                  .map(Map.Entry::getValue)
                  .findFirst()
                  .orElse(null);
    }
}
