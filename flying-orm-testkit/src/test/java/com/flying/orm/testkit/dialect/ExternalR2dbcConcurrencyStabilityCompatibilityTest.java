package com.flying.orm.testkit.dialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.testkit.concurrent.ReactiveConcurrencyProbe;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.pool.PoolMetrics;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.R2dbcTimeoutException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 用真实 MySQL、PostgreSQL、Oracle 和 SQL Server 以及小容量 R2DBC 连接池检查资源边界。
 *
 * <p>这里故意把连接池做得很小，这样不用把本机压到很高负载也能稳定复现池满、等待和归还。慢消费者测试
 * 只断言订阅者看到的 Reactive Streams 行为以及取消后的连接归还，不会声称数据库驱动内部完全没有预取。</p>
 *
 * <p>没有配置外部 URL 时测试会跳过，普通开发构建不需要启动 Docker。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
class ExternalR2dbcConcurrencyStabilityCompatibilityTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final Duration POOL_SETTLE_TIMEOUT = Duration.ofSeconds(5);

    @Test
    void verifiesMysqlPoolExhaustionTimeoutAndRecoveryWhenConfigured() {
        verifyPoolExhaustionAndRecovery("flying.orm.compat.mysql.url", "mysql-real-pool");
    }

    @Test
    void verifiesPostgresqlPoolExhaustionTimeoutAndRecoveryWhenConfigured() {
        verifyPoolExhaustionAndRecovery("flying.orm.compat.postgresql.url", "postgresql-real-pool");
    }

    /** Oracle 连接池也必须在自己的等待上限后失败，池恢复后同一个执行器应能继续使用。 */
    @Test
    void verifiesOraclePoolExhaustionTimeoutAndRecoveryWhenConfigured() {
        verifyPoolExhaustionAndRecovery("flying.orm.compat.oracle.url", "oracle-real-pool");
    }

    /** SQL Server 使用与主线数据库相同的两连接小池，避免预览方言悄悄绕过资源保护。 */
    @Test
    void verifiesSqlServerPoolExhaustionTimeoutAndRecoveryWhenConfigured() {
        verifyPoolExhaustionAndRecovery("flying.orm.compat.sqlserver.url", "sqlserver-real-pool");
    }

    @Test
    void verifiesMysqlSlowConsumerCancellationWhenConfigured() {
        verifySlowConsumerCancellation("flying.orm.compat.mysql.url",
                                       "`FLYING_ORM_STREAM_MYSQL`",
                                       "drop table if exists `FLYING_ORM_STREAM_MYSQL`",
                                       "mysql-real-stream");
    }

    @Test
    void verifiesPostgresqlSlowConsumerCancellationWhenConfigured() {
        verifySlowConsumerCancellation("flying.orm.compat.postgresql.url",
                                       "\"FLYING_ORM_STREAM_PG\"",
                                       "drop table if exists \"FLYING_ORM_STREAM_PG\"",
                                       "postgresql-real-stream");
    }

    /**
     * Oracle 没有通用的 {@code DROP TABLE IF EXISTS}。清理函数只吞掉测试前后的删表失败，建表、写入、
     * 分段拉取和取消中的任何错误仍会让认证失败。
     */
    @Test
    void verifiesOracleSlowConsumerCancellationWhenConfigured() {
        verifySlowConsumerCancellation("flying.orm.compat.oracle.url",
                                       "\"FLYING_ORM_STREAM_ORACLE\"",
                                       "drop table \"FLYING_ORM_STREAM_ORACLE\"",
                                       "oracle-real-stream");
    }

    /** SQL Server 默认开启 QUOTED_IDENTIFIER，测试表仍使用双引号，和正式方言渲染保持一致。 */
    @Test
    void verifiesSqlServerSlowConsumerCancellationWhenConfigured() {
        verifySlowConsumerCancellation("flying.orm.compat.sqlserver.url",
                                       "\"FLYING_ORM_STREAM_SQLSERVER\"",
                                       "drop table if exists \"FLYING_ORM_STREAM_SQLSERVER\"",
                                       "sqlserver-real-stream");
    }

    @Test
    void verifiesMysqlSustainedBoundedConcurrencyWhenConfigured() {
        verifySustainedBoundedConcurrency("flying.orm.compat.mysql.url", "mysql-real-concurrency");
    }

    @Test
    void verifiesPostgresqlSustainedBoundedConcurrencyWhenConfigured() {
        verifySustainedBoundedConcurrency("flying.orm.compat.postgresql.url",
                                          "postgresql-real-concurrency");
    }

    /** Oracle 连续 96 次查询也只能在四条连接内流动，结束后不能留下借出或等待申请。 */
    @Test
    void verifiesOracleSustainedBoundedConcurrencyWhenConfigured() {
        verifySustainedBoundedConcurrency("flying.orm.compat.oracle.url", "oracle-real-concurrency");
    }

    /** SQL Server 连续并发使用同一套峰值和归还断言，结果可以直接和其他数据库横向比较。 */
    @Test
    void verifiesSqlServerSustainedBoundedConcurrencyWhenConfigured() {
        verifySustainedBoundedConcurrency("flying.orm.compat.sqlserver.url",
                                          "sqlserver-real-concurrency");
    }

    /**
     * 先借走池里全部连接，再确认连接池自己的等待上限会结束申请。ORM 只借用连接，不在池外重复管理
     * 排队超时；失败后还要确认等待申请被清除，而不是日后偷偷拿走连接。
     */
    private static void verifyPoolExhaustionAndRecovery(String urlProperty, String poolName) {
        String url = configuredUrl(urlProperty);
        Duration acquireTimeout = Duration.ofMillis(250);
        ConnectionPool pool = newPool(url, poolName, 2, acquireTimeout);
        Connection first = null;
        Connection second = null;
        try {
            first = pool.create().block(TIMEOUT);
            second = pool.create().block(TIMEOUT);
            assertNotNull(first);
            assertNotNull(second);
            assertEquals(2, metrics(pool).acquiredSize());

            R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(pool);
            SqlExecutionOptions options = SqlExecutionOptions.unlimited();
            RdbException failure = assertThrows(
                    RdbException.class,
                    () -> executor.query(oneRowQuery(), options).collectList().block(TIMEOUT));
            assertInstanceOf(R2dbcTimeoutException.class, failure.getCause());

            awaitPool(pool,
                      () -> metrics(pool).pendingAcquireSize() == 0,
                      "连接获取超时后，连接池等待队列没有及时清空");

            // 归还一条就足够让下一次查询继续，证明前一个超时请求没有抢走刚释放的连接。
            Mono.from(first.close()).block(TIMEOUT);
            first = null;
            assertEquals(1, executor.query(oneRowQuery(), options).collectList().block(TIMEOUT).size());
            awaitPool(pool,
                      () -> metrics(pool).acquiredSize() == 1 && metrics(pool).pendingAcquireSize() == 0,
                      "ORM 查询结束后没有只留下测试故意占住的那一条连接");

            // 最后一条占位连接也归还后，池才应该完全空闲。
            Mono.from(second.close()).block(TIMEOUT);
            second = null;
            awaitPoolIdle(pool);
        } finally {
            closeQuietly(first);
            closeQuietly(second);
            closePool(pool);
        }
    }

    /**
     * StepVerifier 从 0 demand 开始，先确认下游没有要数据时订阅者收不到数据，再分两次只要五行并取消。
     * 查询本身仍然有剩余记录，因此连接能否回到池里取决于取消信号有没有穿过执行器的 usingWhen 边界。
     */
    private static void verifySlowConsumerCancellation(String urlProperty,
                                                       String table,
                                                       String cleanupSql,
                                                       String poolName) {
        String url = configuredUrl(urlProperty);
        ConnectionPool pool = newPool(url, poolName, 2);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(pool);
        try {
            cleanup(executor, cleanupSql);
            executor.rowsUpdated(SqlRequest.nativeSql(
                    "create table " + table + " (ID integer primary key, VALUE integer not null)",
                    List.of())).block(TIMEOUT);
            List<List<Object>> seedRows = streamRows(64);
            executor.writeBatch(new BatchWriteRequest(
                    "insert into " + table + " (ID, VALUE) values (?, ?)",
                    2,
                    List.of(Integer.class, Integer.class),
                    SqlBindMarkerStyle.CANONICAL,
                    Flux.fromIterable(seedRows).map(List::toArray),
                    BatchWriteOptions.atomic(seedRows.size()))).block(TIMEOUT);

            // 本场景只认证背压取消和连接归还，不能让完整结果总时限混入同一个失败边界。
            SqlExecutionOptions options = SqlExecutionOptions.unlimited();
            StepVerifier.create(executor.query(SqlRequest.nativeSql(
                                               "select ID, VALUE from " + table + " order by ID",
                                               List.of()), options),
                                0)
                        .expectSubscription()
                        .expectNoEvent(Duration.ofMillis(100))
                        .thenRequest(1)
                        .expectNextCount(1)
                        .thenRequest(4)
                        .expectNextCount(4)
                        .thenCancel()
                        .verify(TIMEOUT);

            awaitPoolIdle(pool);
            Long remaining = executor.query(SqlRequest.nativeSql(
                                     "select count(*) as FLYING_COUNT from " + table,
                                             List.of()), options)
                                     // MySQL 保留大写别名，PostgreSQL 会把未引用别名折成小写。这里只查询一列，
                                     // 按唯一列值读取能让本测试专注取消和资源归还，不混入标识符大小写规则。
                                     .map(row -> ((Number) row.values().iterator().next()).longValue())
                                     .single()
                                     .block(TIMEOUT);
            assertEquals(64L, remaining);
        } finally {
            cleanup(executor, cleanupSql);
            closePool(pool);
        }
    }

    /**
     * 操作数足够让连接反复借出和归还，但保持在轻量认证范围。探针统计调用层并发，PoolCurve 同时记录真实池
     * 在操作开始、成功、失败或取消时看到的容量峰值，结束后再等指标稳定到零借出和零等待。
     */
    private static void verifySustainedBoundedConcurrency(String urlProperty, String poolName) {
        String url = configuredUrl(urlProperty);
        int poolSize = 4;
        ConnectionPool pool = newPool(url, poolName, poolSize);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(pool);
        PoolCurve curve = new PoolCurve();
        try {
            SqlExecutionOptions options = SqlExecutionOptions.timeout(Duration.ofSeconds(5));
            ReactiveConcurrencyProbe.Plan plan = new ReactiveConcurrencyProbe.Plan(96, poolSize, TIMEOUT);
            ReactiveConcurrencyProbe.Result result = ReactiveConcurrencyProbe.run(
                    plan,
                    ignored -> Mono.defer(() -> {
                        curve.observe(metrics(pool));
                        return executor.query(oneRowQuery(), options)
                                       .then()
                                       .doFinally(signal -> curve.observe(metrics(pool)));
                    })).block(TIMEOUT.plusSeconds(5));

            assertNotNull(result);
            assertEquals(96, result.completed());
            assertEquals(0, result.failed());
            assertEquals(0, result.cancelled());
            assertFalse(result.timedOut());
            assertTrue(result.maxInFlight() <= poolSize);
            assertTrue(curve.maxAllocated() > 0);
            assertTrue(curve.maxAllocated() <= poolSize);
            assertTrue(curve.maxAcquired() <= poolSize);

            awaitPoolIdle(pool);
            assertEquals(0, metrics(pool).acquiredSize());
            assertEquals(0, metrics(pool).pendingAcquireSize());
        } finally {
            closePool(pool);
        }
    }

    private static String configuredUrl(String property) {
        String url = System.getProperty(property);
        Assumptions.assumeTrue(url != null && !url.isBlank(), property + " is not configured");
        return url;
    }

    private static ConnectionPool newPool(String url, String name, int maxSize) {
        return newPool(url, name, maxSize, Duration.ofSeconds(10));
    }

    /** 连接排队上限属于连接池配置，测试可按场景使用不同边界。 */
    private static ConnectionPool newPool(String url,
                                          String name,
                                          int maxSize,
                                          Duration acquireTimeout) {
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration
                .builder(ConnectionFactories.get(url))
                .name(name)
                .initialSize(0)
                .minIdle(0)
                .maxSize(maxSize)
                .maxAcquireTime(acquireTimeout)
                .maxIdleTime(Duration.ofMinutes(1))
                .build();
        return new ConnectionPool(configuration);
    }

    private static PoolMetrics metrics(ConnectionPool pool) {
        return pool.getMetrics().orElseThrow(() -> new AssertionError("R2DBC 连接池没有提供资源指标"));
    }

    private static SqlRequest oneRowQuery() {
        return SqlRequest.nativeSql("select 1 as FLYING_VALUE", List.of());
    }

    private static List<List<Object>> streamRows(int count) {
        List<List<Object>> rows = new ArrayList<>(count);
        for (int index = 1; index <= count; index++) {
            rows.add(List.of(index, index * 10));
        }
        return rows;
    }

    private static void awaitPoolIdle(ConnectionPool pool) {
        awaitPool(pool,
                  () -> metrics(pool).acquiredSize() == 0 && metrics(pool).pendingAcquireSize() == 0,
                  "响应式操作结束后，连接池仍有借出连接或等待申请");
    }

    /**
     * 这里只暂停 JUnit 测试线程，不会阻塞 R2DBC 事件循环。池归还是异步完成的，短轮询比写死一个大睡眠更快，
     * 超时信息也比偶发地立刻读取指标更容易排查。
     */
    private static void awaitPool(ConnectionPool pool, BooleanSupplier condition, String message) {
        long deadline = System.nanoTime() + POOL_SETTLE_TIMEOUT.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError("等待连接池资源归还时测试线程被中断", error);
            }
        }
        PoolMetrics snapshot = metrics(pool);
        assertTrue(condition.getAsBoolean(),
                   () -> message + "，当前 acquired=" + snapshot.acquiredSize()
                           + ", pending=" + snapshot.pendingAcquireSize()
                           + ", allocated=" + snapshot.allocatedSize()
                           + ", idle=" + snapshot.idleSize());
    }

    private static void cleanup(R2dbcSqlExecutor executor, String sql) {
        executor.rowsUpdated(SqlRequest.nativeSql(sql, List.of()))
                .onErrorResume(ignored -> Mono.empty())
                .block(TIMEOUT);
    }

    private static void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        Mono.from(connection.close()).onErrorResume(ignored -> Mono.empty()).block(TIMEOUT);
    }

    private static void closePool(ConnectionPool pool) {
        pool.disposeLater().onErrorResume(ignored -> Mono.empty()).block(TIMEOUT);
    }

    /**
     * 原子计数只保存峰值，不暴露连接对象，也不会让观测动作改变池的调度。
     */
    private static final class PoolCurve {

        private final AtomicInteger maxAllocated = new AtomicInteger();
        private final AtomicInteger maxAcquired = new AtomicInteger();

        private void observe(PoolMetrics current) {
            maxAllocated.accumulateAndGet(current.allocatedSize(), Math::max);
            maxAcquired.accumulateAndGet(current.acquiredSize(), Math::max);
        }

        private int maxAllocated() {
            return maxAllocated.get();
        }

        private int maxAcquired() {
            return maxAcquired.get();
        }
    }
}
