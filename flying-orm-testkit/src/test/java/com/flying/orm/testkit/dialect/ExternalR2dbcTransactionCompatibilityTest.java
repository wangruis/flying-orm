package com.flying.orm.testkit.dialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 在真实数据库上验证批量事务和错误分类。MySQL/PostgreSQL 是正式认证范围，Oracle/SQL Server
 * 先按同一事务契约做预览认证，不能因为方言处于预览状态就放松回滚和结果可见性要求。
 *
 * <p>这里故意不使用故障注入器。重复键由数据库真实产生，回滚和部分提交也由真实事务完成；否则只能证明
 * testkit 会返回预先写好的结果，不能证明驱动、连接和 flying-orm 的事务协作正确。</p>
 *
 * <p>没有配置外部 URL 时测试会跳过，普通开发构建不需要先启动 Docker。认证脚本会注入 URL 并收集报告。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
class ExternalR2dbcTransactionCompatibilityTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Test
    void verifiesMysqlBatchTransactionsAndDuplicateClassificationWhenConfigured() {
        verifyBatchTransactions("flying.orm.compat.mysql.url",
                                "`FLYING_ORM_TX_MYSQL`",
                                "drop table if exists `FLYING_ORM_TX_MYSQL`",
                                "`");
    }

    @Test
    void verifiesPostgresqlBatchTransactionsAndDuplicateClassificationWhenConfigured() {
        verifyBatchTransactions("flying.orm.compat.postgresql.url",
                                "\"FLYING_ORM_TX_PG\"",
                                "drop table if exists \"FLYING_ORM_TX_PG\"",
                                "\"");
    }

    @Test
    void verifiesOracleBatchTransactionsAndDuplicateClassificationWhenConfigured() {
        verifyBatchTransactions("flying.orm.compat.oracle.url",
                                "\"FLYING_ORM_TX_ORACLE\"",
                                "drop table \"FLYING_ORM_TX_ORACLE\"",
                                "\"");
    }

    @Test
    void verifiesSqlServerBatchTransactionsAndDuplicateClassificationWhenConfigured() {
        verifyBatchTransactions("flying.orm.compat.sqlserver.url",
                                "\"FLYING_ORM_TX_SQLSERVER\"",
                                "drop table if exists \"FLYING_ORM_TX_SQLSERVER\"",
                                "\"");
    }

    @Test
    void verifiesMysqlTimeoutCancellationWhenConfigured() {
        verifyTimeoutCancellation("flying.orm.compat.mysql.url", "select sleep(5) as FLYING_DELAY");
    }

    @Test
    void verifiesPostgresqlTimeoutCancellationWhenConfigured() {
        verifyTimeoutCancellation("flying.orm.compat.postgresql.url", "select pg_sleep(5) as FLYING_DELAY");
    }

    @Test
    void verifiesMysqlNowaitLockClassificationWhenConfigured() {
        verifyNowaitLock("flying.orm.compat.mysql.url",
                         "`FLYING_ORM_LOCK_MYSQL`",
                         "drop table if exists `FLYING_ORM_LOCK_MYSQL`",
                         "`");
    }

    @Test
    void verifiesPostgresqlNowaitLockClassificationWhenConfigured() {
        verifyNowaitLock("flying.orm.compat.postgresql.url",
                         "\"FLYING_ORM_LOCK_PG\"",
                         "drop table if exists \"FLYING_ORM_LOCK_PG\"",
                         "\"");
    }

    /**
     * 三行输入把失败放在中间：第一行可以先成功，第二行触发重复键，第三行用于确认失败后是否继续。
     * 这个布局能同时区分 ATOMIC 的整批回滚和 INDEPENDENT 的分片隔离。
     */
    private static void verifyBatchTransactions(String urlProperty,
                                                String table,
                                                String cleanupSql,
                                                String identifierQuote) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");

        ConnectionFactory connectionFactory = ConnectionFactories.get(url);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        String idColumn = identifierQuote + "ID" + identifierQuote;
        String nameColumn = identifierQuote + "NAME" + identifierQuote;
        String createSql = "create table " + table + " (" + idColumn + " integer primary key, "
                + nameColumn + " varchar(64) not null)";
        String insertSql = "insert into " + table + " (" + idColumn + ", " + nameColumn + ") values (?, ?)";

        cleanup(executor, cleanupSql)
                .then(executor.rowsUpdated(SqlRequest.nativeSql(createSql, List.of())))
                .block(TIMEOUT);
        try {
            BatchWriteException atomicError = assertThrows(
                    BatchWriteException.class,
                    () -> executor.writeBatch(batch(insertSql, BatchWriteOptions.atomic(1))).block(TIMEOUT));
            assertEquals(BatchWriteResult.Status.ROLLED_BACK, atomicError.result().status());
            assertEquals(0L, count(executor, table));

            List<BatchChunkResult> chunks = executor.writeBatchChunks(
                    batch(insertSql, BatchWriteOptions.independent(1)))
                    .collectList()
                    .block(TIMEOUT);
            assertNotNull(chunks);
            assertEquals(List.of(BatchChunkResult.Status.COMMITTED,
                                 BatchChunkResult.Status.FAILED,
                                 BatchChunkResult.Status.COMMITTED),
                         chunks.stream().map(BatchChunkResult::status).toList());
            assertEquals(List.of(1, 2), ids(executor, table, idColumn));

            RdbException duplicate = assertThrows(
                    RdbException.class,
                    () -> executor.rowsUpdated(new SqlRequest(insertSql, List.of(1, "again"))).block(TIMEOUT));
            assertEquals(RdbErrorKind.DUPLICATE_KEY, duplicate.kind());
            assertNotNull(duplicate.getCause());
        } finally {
            cleanup(executor, cleanupSql).block(TIMEOUT);
        }
    }

    private static BatchWriteRequest batch(String sql, BatchWriteOptions options) {
        return new BatchWriteRequest(sql,
                                     2,
                                     List.of(Integer.class, String.class),
                                     SqlBindMarkerStyle.CANONICAL,
                                     Flux.just(new Object[]{1, "first"},
                                               new Object[]{1, "duplicate"},
                                               new Object[]{2, "last"}),
                                     options);
    }

    /**
     * 超时不仅要返回明确异常，还要取消原来的结果流。紧接着执行一个轻查询，至少能证明执行器没有因为前一次
     * 取消而停留在不可用状态；连接池耗尽和长时间资源曲线会放到后续压力批次单独量化。
     */
    private static void verifyTimeoutCancellation(String urlProperty, String slowSql) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");

        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(ConnectionFactories.get(url));
        SqlExecutionOptions options = SqlExecutionOptions.timeout(Duration.ofMillis(300));
        assertThrows(SqlExecutionTimeoutException.class,
                     () -> executor.query(SqlRequest.nativeSql(slowSql, List.of()), options)
                                   .then()
                                   .block(Duration.ofSeconds(5)));

        Map<String, Object> row = executor.query(SqlRequest.nativeSql("select 1 as FLYING_VALUE", List.of()))
                                          .single()
                                          .block(TIMEOUT);
        assertEquals(1, ((Number) value(row, "FLYING_VALUE")).intValue());
    }

    /**
     * 第一条连接显式开启事务并锁住一行，第二条连接通过 flying-orm 执行 NOWAIT 查询。NOWAIT 让测试不依赖
     * 数据库默认锁等待时间，也不会因为机器快慢不同而偶发超时。
     */
    private static void verifyNowaitLock(String urlProperty,
                                         String table,
                                         String cleanupSql,
                                         String identifierQuote) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");

        ConnectionFactory connectionFactory = ConnectionFactories.get(url);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        String idColumn = identifierQuote + "ID" + identifierQuote;
        String createSql = "create table " + table + " (" + idColumn + " integer primary key)";
        String lockSql = "select " + idColumn + " from " + table + " where " + idColumn + " = 1 for update";
        String nowaitSql = lockSql + " nowait";

        cleanup(executor, cleanupSql)
                .then(executor.rowsUpdated(SqlRequest.nativeSql(createSql, List.of())))
                .then(executor.rowsUpdated(SqlRequest.nativeSql(
                        "insert into " + table + " (" + idColumn + ") values (1)", List.of())))
                .block(TIMEOUT);
        try {
            Mono<Void> conflict = Mono.usingWhen(
                    Mono.from(connectionFactory.create()),
                    holder -> Mono.from(holder.beginTransaction())
                                  .then(lockRow(holder, lockSql))
                                  .then(executor.query(SqlRequest.nativeSql(nowaitSql, List.of())).then()),
                    ExternalR2dbcTransactionCompatibilityTest::rollbackAndClose,
                    (holder, ignored) -> rollbackAndClose(holder),
                    ExternalR2dbcTransactionCompatibilityTest::rollbackAndClose);

            RdbException lockError = assertThrows(RdbException.class, () -> conflict.block(TIMEOUT));
            assertEquals(RdbErrorKind.LOCK_TIMEOUT, lockError.kind());
            assertNotNull(lockError.getCause());
        } finally {
            cleanup(executor, cleanupSql).block(TIMEOUT);
        }
    }

    private static Mono<Void> lockRow(Connection connection, String sql) {
        // SELECT 必须把 Result 消费完才真正拿到行锁，不能只创建 Statement 就开始第二条连接。
        return Flux.from(connection.createStatement(sql).execute())
                   .flatMap(result -> result.map((row, metadata) -> row.get(0)))
                   .then();
    }

    private static Mono<Void> rollbackAndClose(Connection connection) {
        return Mono.from(connection.rollbackTransaction())
                   .onErrorResume(ignored -> Mono.empty())
                   .then(Mono.from(connection.close()));
    }

    private static long count(R2dbcSqlExecutor executor, String table) {
        Map<String, Object> row = executor.query(SqlRequest.nativeSql(
                                                  "select count(*) as FLYING_COUNT from " + table,
                                                  List.of()))
                                          .single()
                                          .block(TIMEOUT);
        return ((Number) value(row, "FLYING_COUNT")).longValue();
    }

    private static List<Integer> ids(R2dbcSqlExecutor executor, String table, String idColumn) {
        return executor.query(SqlRequest.nativeSql("select " + idColumn + " from " + table + " order by " + idColumn,
                                                   List.of()))
                       .map(row -> ((Number) value(row, "ID")).intValue())
                       .collectList()
                       .block(TIMEOUT);
    }

    private static Mono<Long> cleanup(R2dbcSqlExecutor executor, String cleanupSql) {
        return executor.rowsUpdated(SqlRequest.nativeSql(cleanupSql, List.of()))
                       .onErrorResume(ignored -> Mono.empty());
    }

    private static Object value(Map<String, Object> row, String name) {
        return row.entrySet()
                  .stream()
                  .filter(entry -> entry.getKey().equalsIgnoreCase(name))
                  .map(Map.Entry::getValue)
                  .findFirst()
                  .orElseThrow(() -> new AssertionError("missing column " + name + " in " + row.keySet()));
    }
}
