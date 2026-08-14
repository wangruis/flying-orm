package com.flying.orm.testkit.dialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 在真实 MySQL、PostgreSQL、Oracle 和 SQL Server 上制造死锁、连接中断和提交确认丢失。
 *
 * <p>这些场景不靠预先写好的异常冒充数据库故障。死锁和会话终止由数据库自己执行；UNKNOWN 场景也会先让
 * 数据库完成真实提交，只在提交成功后的确认边界关闭连接。这样才能同时检查驱动异常、事务释放和恢复回执。</p>
 *
 * <p>没有配置外部 URL 时测试会跳过，普通开发构建不需要启动 Docker。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
class ExternalR2dbcFailureRecoveryCompatibilityTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Test
    void verifiesMysqlDeadlockClassificationWhenConfigured() {
        verifyDeadlock("flying.orm.compat.mysql.url",
                       "`FLYING_ORM_DEADLOCK_MYSQL`",
                       "drop table if exists `FLYING_ORM_DEADLOCK_MYSQL`",
                       "`");
    }

    @Test
    void verifiesPostgresqlDeadlockClassificationWhenConfigured() {
        verifyDeadlock("flying.orm.compat.postgresql.url",
                       "\"FLYING_ORM_DEADLOCK_PG\"",
                       "drop table if exists \"FLYING_ORM_DEADLOCK_PG\"",
                       "\"");
    }

    /** Oracle 自己挑选死锁受害者，测试只检查驱动异常能否稳定翻译成 DEADLOCK。 */
    @Test
    void verifiesOracleDeadlockClassificationWhenConfigured() {
        verifyDeadlock("flying.orm.compat.oracle.url",
                       "\"FLYING_ORM_DEADLOCK_ORACLE\"",
                       "drop table \"FLYING_ORM_DEADLOCK_ORACLE\"",
                       "\"");
    }

    /** SQL Server 的 1205 错误必须和其他数据库落到同一个稳定错误分类。 */
    @Test
    void verifiesSqlServerDeadlockClassificationWhenConfigured() {
        verifyDeadlock("flying.orm.compat.sqlserver.url",
                       "\"FLYING_ORM_DEADLOCK_SQLSERVER\"",
                       "drop table if exists \"FLYING_ORM_DEADLOCK_SQLSERVER\"",
                       "\"");
    }

    /** Oracle 必须真的等待一秒后返回 ORA-30006，不能拿 NOWAIT 的立即失败代替等待超时。 */
    @Test
    void verifiesOracleExplicitLockTimeoutWhenConfigured() {
        verifyExplicitLockTimeout("flying.orm.compat.oracle.url",
                                  "\"FLYING_ORM_LOCK_TIMEOUT_ORACLE\"",
                                  "drop table \"FLYING_ORM_LOCK_TIMEOUT_ORACLE\"",
                                  "\"",
                                  null,
                                  true,
                                  30006);
    }

    /** SQL Server 的 LOCK_TIMEOUT 只影响当前会话，所以必须在实际发起竞争更新的连接上设置。 */
    @Test
    void verifiesSqlServerExplicitLockTimeoutWhenConfigured() {
        verifyExplicitLockTimeout("flying.orm.compat.sqlserver.url",
                                  "\"FLYING_ORM_LOCK_TIMEOUT_SQLSERVER\"",
                                  "drop table if exists \"FLYING_ORM_LOCK_TIMEOUT_SQLSERVER\"",
                                  "\"",
                                  "set lock_timeout 500",
                                  false,
                                  1222);
    }

    @Test
    void verifiesMysqlConnectionInterruptionWhenConfigured() {
        verifyConnectionInterruption("flying.orm.compat.mysql.url",
                                     "select connection_id() as FLYING_SESSION_ID",
                                     "select sleep(10) as FLYING_DELAY",
                                     SessionTermination.MYSQL,
                                     null);
    }

    @Test
    void verifiesPostgresqlConnectionInterruptionWhenConfigured() {
        verifyConnectionInterruption("flying.orm.compat.postgresql.url",
                                     "select pg_backend_pid() as FLYING_SESSION_ID",
                                     "select pg_sleep(10) as FLYING_DELAY",
                                     SessionTermination.POSTGRESQL,
                                     null);
    }

    /** Oracle 使用业务会话执行休眠，由独立 SYSTEM 连接按 SID、SERIAL# 终止，业务账号不扩权。 */
    @Test
    void verifiesOracleConnectionInterruptionWhenConfigured() {
        verifyConnectionInterruption("flying.orm.compat.oracle.url",
                                     "select sys_context('USERENV', 'SID') as FLYING_SESSION_ID from dual",
                                     "begin dbms_session.sleep(10); end;",
                                     SessionTermination.ORACLE,
                                     "flying.orm.compat.oracle.admin.url");
    }

    /** SQL Server 使用当前连接的 SPID，KILL 后必须返回 CONNECTION，并且新连接仍能马上查询。 */
    @Test
    void verifiesSqlServerConnectionInterruptionWhenConfigured() {
        verifyConnectionInterruption("flying.orm.compat.sqlserver.url",
                                     "select @@spid as FLYING_SESSION_ID",
                                     "waitfor delay '00:00:10'",
                                     SessionTermination.SQL_SERVER,
                                     null);
    }

    @Test
    void verifiesMysqlReceiptConfirmationAndUnknownRecoveryWhenConfigured() {
        verifyReceiptConfirmationAndUnknownRecovery("flying.orm.compat.mysql.url",
                                                    "`FLYING_ORM_RECOVERY_MYSQL`",
                                                    "drop table if exists `FLYING_ORM_RECOVERY_MYSQL`",
                                                    "`",
                                                    "mysql-real-unknown-recovery",
                                                    "drop table if exists flying_orm_batch_receipt",
                                                    commonReceiptTableSql());
    }

    @Test
    void verifiesPostgresqlReceiptConfirmationAndUnknownRecoveryWhenConfigured() {
        verifyReceiptConfirmationAndUnknownRecovery("flying.orm.compat.postgresql.url",
                                                    "\"FLYING_ORM_RECOVERY_PG\"",
                                                    "drop table if exists \"FLYING_ORM_RECOVERY_PG\"",
                                                    "\"",
                                                    "postgresql-real-unknown-recovery",
                                                    "drop table if exists flying_orm_batch_receipt",
                                                    commonReceiptTableSql());
    }

    /** Oracle 回执表使用 NUMBER，避免把 64 位计数是否接受 BIGINT 混进 UNKNOWN 语义认证。 */
    @Test
    void verifiesOracleReceiptConfirmationAndUnknownRecoveryWhenConfigured() {
        verifyReceiptConfirmationAndUnknownRecovery("flying.orm.compat.oracle.url",
                                                    "\"FLYING_ORM_RECOVERY_ORACLE\"",
                                                    "drop table \"FLYING_ORM_RECOVERY_ORACLE\"",
                                                    "\"",
                                                    "oracle-real-unknown-recovery",
                                                    "drop table flying_orm_batch_receipt",
                                                    oracleReceiptTableSql());
    }

    /** SQL Server 的 timestamp 是 rowversion，所以回执时间显式使用 datetime2。 */
    @Test
    void verifiesSqlServerReceiptConfirmationAndUnknownRecoveryWhenConfigured() {
        verifyReceiptConfirmationAndUnknownRecovery("flying.orm.compat.sqlserver.url",
                                                    "\"FLYING_ORM_RECOVERY_SQLSERVER\"",
                                                    "drop table if exists \"FLYING_ORM_RECOVERY_SQLSERVER\"",
                                                    "\"",
                                                    "sqlserver-real-unknown-recovery",
                                                    "drop table if exists flying_orm_batch_receipt",
                                                    sqlServerReceiptTableSql());
    }

    /**
     * 两个事务先各拿一把锁，再同时去拿对方的锁。数据库必须挑一个事务作为死锁受害者；受害者一报错就立即
     * 回滚，否则另一个事务会继续等着它释放第一把锁，测试自己也会卡住。
     */
    private static void verifyDeadlock(String urlProperty,
                                       String table,
                                       String cleanupSql,
                                       String identifierQuote) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");

        ConnectionFactory connectionFactory = ConnectionFactories.get(url);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        String idColumn = identifierQuote + "ID" + identifierQuote;
        String valueColumn = identifierQuote + "VALUE" + identifierQuote;
        String createSql = "create table " + table + " (" + idColumn + " integer primary key, "
                + valueColumn + " integer not null)";

        cleanup(executor, cleanupSql)
                .then(executor.rowsUpdated(SqlRequest.nativeSql(createSql, List.of())))
                // 两条普通 INSERT 四库都支持。这里不使用多行 VALUES，避免 Oracle 语法差异抢先终止死锁测试。
                .then(executor.rowsUpdated(SqlRequest.nativeSql(
                        "insert into " + table + " (" + idColumn + ", " + valueColumn + ") values (1, 0)",
                        List.of())))
                .then(executor.rowsUpdated(SqlRequest.nativeSql(
                        "insert into " + table + " (" + idColumn + ", " + valueColumn + ") values (2, 0)",
                        List.of())))
                .block(TIMEOUT);

        Connection first = Mono.from(connectionFactory.create()).block(TIMEOUT);
        Connection second = Mono.from(connectionFactory.create()).block(TIMEOUT);
        assertNotNull(first);
        assertNotNull(second);
        try {
            Mono.from(first.beginTransaction())
                .then(Mono.from(second.beginTransaction()))
                // UPDATE 在事务结束前持有写锁，四库行为明确，也不会把查询游标释放时机混进死锁测试。
                .then(lockRow(first, updateLockSql(table, valueColumn, idColumn, 1)))
                .then(lockRow(second, updateLockSql(table, valueColumn, idColumn, 2)))
                .block(TIMEOUT);

            List<LockAttempt> attempts = Mono.zip(
                    competingLock(first, updateLockSql(table, valueColumn, idColumn, 2)),
                    competingLock(second, updateLockSql(table, valueColumn, idColumn, 1)))
                                                  .map(pair -> List.of(pair.getT1(), pair.getT2()))
                                                  .block(TIMEOUT);
            assertNotNull(attempts);

            List<RdbException> deadlocks = attempts.stream()
                                                   .map(LockAttempt::error)
                                                   .filter(error -> error != null)
                                                   .map(RdbExceptionTranslator::translate)
                                                   .map(RdbException.class::cast)
                                                   .filter(error -> error.kind() == RdbErrorKind.DEADLOCK)
                                                   .toList();
            assertEquals(1, deadlocks.size(), () -> describeLockAttempts(attempts));
            assertInstanceOf(R2dbcException.class, deadlocks.getFirst().getCause());
        } finally {
            // 死锁受害事务已经在 competingLock 里回滚。SQL Server 驱动对同一连接再次 rollback 可能一直不结束，
            // 两条连接并行做有界清理，既不遮住前面的错误分类结果，也不会让幸存事务继续占着另一把锁。
            Mono.whenDelayError(rollbackAndClose(first), rollbackAndClose(second))
                    .then(cleanup(executor, cleanupSql))
                    .block(TIMEOUT);
        }
    }

    private static String updateLockSql(String table, String valueColumn, String idColumn, int id) {
        return "update " + table + " set " + valueColumn + " = " + valueColumn + " + 1 where "
                + idColumn + " = " + id;
    }

    /**
     * 第一条连接用 UPDATE 持有行锁，第二条连接按数据库自己的等待超时机制竞争同一行。这里同时检查三件事：
     * 驱动返回的是真实数据库错误码、flying-orm 把它归到 LOCK_TIMEOUT、释放两个事务后新连接还能立即工作。
     */
    private static void verifyExplicitLockTimeout(String urlProperty,
                                                  String table,
                                                  String cleanupSql,
                                                  String identifierQuote,
                                                  String contenderSetupSql,
                                                  boolean conflictUsesSelect,
                                                  int expectedErrorCode) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");

        ConnectionFactory connectionFactory = ConnectionFactories.get(url);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        String idColumn = identifierQuote + "ID" + identifierQuote;
        String valueColumn = identifierQuote + "VALUE" + identifierQuote;
        String createSql = "create table " + table + " (" + idColumn + " integer primary key, "
                + valueColumn + " integer not null)";
        String updateSql = updateLockSql(table, valueColumn, idColumn, 1);
        String conflictSql = conflictUsesSelect
                ? "select " + idColumn + " from " + table + " where " + idColumn + " = 1 for update wait 1"
                : updateSql;

        cleanup(executor, cleanupSql)
                .then(executor.rowsUpdated(SqlRequest.nativeSql(createSql, List.of())))
                .then(executor.rowsUpdated(SqlRequest.nativeSql(
                        "insert into " + table + " (" + idColumn + ", " + valueColumn + ") values (1, 0)",
                        List.of())))
                .block(TIMEOUT);
        try {
            Connection holder = Mono.from(connectionFactory.create()).block(TIMEOUT);
            Connection contender = Mono.from(connectionFactory.create()).block(TIMEOUT);
            assertNotNull(holder);
            assertNotNull(contender);
            try {
                Mono.from(holder.beginTransaction())
                    .then(Mono.from(contender.beginTransaction()))
                    .then(lockRow(holder, updateSql))
                    .then(contenderSetupSql == null ? Mono.empty() : lockRow(contender, contenderSetupSql))
                    .block(TIMEOUT);

                Mono<Void> conflict = conflictUsesSelect
                        ? readAndDiscard(contender, conflictSql)
                        : lockRow(contender, conflictSql);
                RdbException lockError = assertThrows(
                        RdbException.class,
                        () -> conflict.onErrorMap(RdbExceptionTranslator::translate).block(TIMEOUT));
                assertEquals(RdbErrorKind.LOCK_TIMEOUT, lockError.kind(), () -> describeThrowable(lockError));
                R2dbcException driverError = assertInstanceOf(R2dbcException.class, lockError.getCause());
                assertEquals(expectedErrorCode,
                             driverError.getErrorCode(),
                             () -> "锁等待超时返回了意外的数据库错误：" + describeThrowable(driverError));
            } finally {
                Mono.whenDelayError(rollbackAndClose(holder), rollbackAndClose(contender)).block(TIMEOUT);
            }

            // 竞争失败不能污染后续连接；能马上读到原行，说明事务和锁都已经清干净。
            Map<String, Object> recovered = executor.query(SqlRequest.nativeSql(
                                                    "select count(*) as FLYING_COUNT from " + table,
                                                    List.of()))
                                                    .single()
                                                    .block(TIMEOUT);
            assertNotNull(recovered);
            assertEquals(1, ((Number) rowValue(recovered, "FLYING_COUNT")).intValue());
        } finally {
            cleanup(executor, cleanupSql).block(TIMEOUT);
        }
    }

    private static Mono<Void> readAndDiscard(Connection connection, String sql) {
        // SELECT 只有消费 Result 后才会真正等待行锁；只调用 execute() 会让测试在锁竞争发生前就结束。
        return Flux.from(connection.createStatement(sql).execute())
                   .flatMap(result -> result.map((row, metadata) -> row.get(0)))
                   .then();
    }

    /**
     * 回执和业务数据在同一个真实事务里提交。包装连接不会伪造提交成功，而是等待真实 commit 完成后关闭连接，
     * 再返回连接异常来模拟“数据库已经提交，但调用方没收到确认”的边界。
     */
    private static void verifyReceiptConfirmationAndUnknownRecovery(String urlProperty,
                                                                    String table,
                                                                    String cleanupSql,
                                                                    String identifierQuote,
                                                                    String operationId,
                                                                    String receiptCleanupSql,
                                                                    String receiptCreateSql) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");

        ConnectionFactory connectionFactory = ConnectionFactories.get(url);
        R2dbcSqlExecutor stableExecutor = R2dbcSqlExecutor.create(connectionFactory);
        String idColumn = identifierQuote + "ID" + identifierQuote;
        String nameColumn = identifierQuote + "NAME" + identifierQuote;
        String createSql = "create table " + table + " (" + idColumn + " integer primary key, "
                + nameColumn + " varchar(64) not null)";
        String insertSql = "insert into " + table + " (" + idColumn + ", " + nameColumn + ") values (?, ?)";

        cleanup(stableExecutor, cleanupSql)
                .then(cleanup(stableExecutor, receiptCleanupSql))
                .then(stableExecutor.rowsUpdated(SqlRequest.nativeSql(createSql, List.of())))
                .then(stableExecutor.rowsUpdated(SqlRequest.nativeSql(receiptCreateSql, List.of())))
                .block(TIMEOUT);
        try {
            BatchWriteRequest activelyConfirmedRequest = new BatchWriteRequest(
                    insertSql,
                    2,
                    List.of(Integer.class, String.class),
                    SqlBindMarkerStyle.CANONICAL,
                    Flux.just(new Object[]{1, "first"}, new Object[]{2, "second"}),
                    BatchWriteOptions.atomic(2).withReceipt(operationId));

            BatchWriteResult confirmed = R2dbcSqlExecutor.create(
                            loseFirstCommitAcknowledgement(connectionFactory))
                    .writeBatch(activelyConfirmedRequest)
                    .block(TIMEOUT);
            assertNotNull(confirmed);
            assertEquals(BatchWriteResult.Status.COMMITTED, confirmed.status());
            assertEquals(2L, confirmed.affectedRows());
            assertEquals(2L, count(stableExecutor, table));

            // 同一个 operation id、SQL 和参数再次提交时直接读取回执，不能真的再执行 INSERT。
            BatchWriteResult confirmedReplay = stableExecutor.writeBatch(activelyConfirmedRequest).block(TIMEOUT);
            assertNotNull(confirmedReplay);
            assertEquals(BatchWriteResult.Status.COMMITTED, confirmedReplay.status());
            assertEquals(2L, confirmedReplay.affectedRows());
            assertEquals(2L, count(stableExecutor, table));

            cleanup(stableExecutor, cleanupSql)
                    .then(stableExecutor.rowsUpdated(SqlRequest.nativeSql(createSql, List.of())))
                    .block(TIMEOUT);
            BatchWriteRequest manualRecoveryRequest = new BatchWriteRequest(
                    insertSql,
                    2,
                    List.of(Integer.class, String.class),
                    SqlBindMarkerStyle.CANONICAL,
                    Flux.just(new Object[]{1, "first"}, new Object[]{2, "second"}),
                    BatchWriteOptions.atomic(2).withReceipt(operationId + "-manual", Duration.ZERO));

            BatchWriteException unknown = assertThrows(
                    BatchWriteException.class,
                    () -> R2dbcSqlExecutor.create(loseFirstCommitAcknowledgement(connectionFactory))
                            .writeBatch(manualRecoveryRequest)
                            .block(TIMEOUT));
            assertEquals(BatchWriteResult.Status.UNKNOWN,
                         unknown.result().status(),
                         () -> "批量确认丢失后的实际结果=" + unknown.result()
                                 + ", cause=" + describeThrowable(unknown.getCause()));
            assertEquals(1, unknown.result().chunks().size());
            BatchChunkResult unknownChunk = unknown.result().chunks().getFirst();
            assertEquals(BatchChunkResult.Status.UNKNOWN, unknownChunk.status());
            assertNotNull(unknownChunk.recoveryToken());

            BatchResolution resolution = stableExecutor.resolveUnknown(unknownChunk.recoveryToken()).block(TIMEOUT);
            assertNotNull(resolution);
            assertEquals(BatchResolution.Status.COMMITTED, resolution.status());

            BatchWriteResult replay = stableExecutor.writeBatch(manualRecoveryRequest).block(TIMEOUT);
            assertNotNull(replay);
            assertEquals(BatchWriteResult.Status.COMMITTED, replay.status());
            assertEquals(2L, replay.affectedRows());
            assertEquals(2L, count(stableExecutor, table));
        } finally {
            cleanup(stableExecutor, cleanupSql)
                    .then(cleanup(stableExecutor, receiptCleanupSql))
                    .block(TIMEOUT);
        }
    }

    private static String commonReceiptTableSql() {
        return """
                create table flying_orm_batch_receipt (
                    operation_id varchar(128) not null,
                    chunk_index integer not null,
                    plan_hash varchar(64) not null,
                    payload_hash varchar(64) not null,
                    row_count bigint not null,
                    affected_rows bigint not null,
                    status varchar(32) not null,
                    created_at timestamp not null default current_timestamp,
                    primary key (operation_id, chunk_index)
                )
                """;
    }

    private static String oracleReceiptTableSql() {
        return """
                create table flying_orm_batch_receipt (
                    operation_id varchar2(128) not null,
                    chunk_index integer not null,
                    plan_hash varchar2(64) not null,
                    payload_hash varchar2(64) not null,
                    row_count number(19) not null,
                    affected_rows number(19) not null,
                    status varchar2(32) not null,
                    created_at timestamp default current_timestamp not null,
                    primary key (operation_id, chunk_index)
                )
                """;
    }

    private static String sqlServerReceiptTableSql() {
        return """
                create table flying_orm_batch_receipt (
                    operation_id varchar(128) not null,
                    chunk_index integer not null,
                    plan_hash varchar(64) not null,
                    payload_hash varchar(64) not null,
                    row_count bigint not null,
                    affected_rows bigint not null,
                    status varchar(32) not null,
                    created_at datetime2 default current_timestamp not null,
                    primary key (operation_id, chunk_index)
                )
                """;
    }

    private static ConnectionFactory loseFirstCommitAcknowledgement(ConnectionFactory delegate) {
        AtomicBoolean lost = new AtomicBoolean();
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.from(delegate.create()).map(connection -> proxy(
                        Connection.class,
                        (ignored, method, args) -> {
                            if ("commitTransaction".equals(method.getName()) && lost.compareAndSet(false, true)) {
                                return Mono.from(connection.commitTransaction())
                                           .then(Mono.from(connection.close()).onErrorResume(closeError -> Mono.empty()))
                                           .then(Mono.error(new R2dbcNonTransientResourceException(
                                                   "commit acknowledgement lost after connection closed",
                                                   "08006")));
                            }
                            return invoke(connection, method, args);
                        }));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return delegate.getMetadata();
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException error) {
            throw error.getCause();
        }
    }

    /**
     * 先从受测连接读取数据库分配的会话编号，再把同一条连接交给执行器跑慢查询。管理连接延迟终止这个编号，
     * 因此看到的异常来自数据库和驱动的真实断链过程，不是测试代码主动取消订阅。
     */
    private static void verifyConnectionInterruption(String urlProperty,
                                                     String sessionIdSql,
                                                     String slowSql,
                                                     SessionTermination termination,
                                                     String adminUrlProperty) {
        String url = System.getProperty(urlProperty);
        Assumptions.assumeTrue(url != null && !url.isBlank(), urlProperty + " is not configured");
        String adminUrl = adminUrlProperty == null ? url : System.getProperty(adminUrlProperty);
        Assumptions.assumeTrue(adminUrl != null && !adminUrl.isBlank(),
                               adminUrlProperty == null ? urlProperty + " is not configured"
                                       : adminUrlProperty + " is not configured");

        ConnectionFactory connectionFactory = ConnectionFactories.get(url);
        R2dbcSqlExecutor stableExecutor = R2dbcSqlExecutor.create(connectionFactory);
        R2dbcSqlExecutor adminExecutor = R2dbcSqlExecutor.create(ConnectionFactories.get(adminUrl));
        Connection victim = Mono.from(connectionFactory.create()).block(TIMEOUT);
        assertNotNull(victim);
        long sessionId = readLong(victim, sessionIdSql);
        R2dbcSqlExecutor victimExecutor = R2dbcSqlExecutor.create(singleConnection(connectionFactory, victim));

        Mono<Void> slowQuery = termination.executesSlowStatementAsUpdate()
                ? victimExecutor.rowsUpdated(SqlRequest.nativeSql(slowSql, List.of())).then()
                : victimExecutor.query(SqlRequest.nativeSql(slowSql, List.of())).then();
        Mono<Void> terminate = Mono.delay(Duration.ofMillis(300))
                                   .then(terminateSession(adminExecutor, sessionId, termination));

        RdbException connectionError = assertInstanceOf(
                RdbException.class,
                assertThrows(
                        RuntimeException.class,
                        () -> Mono.whenDelayError(slowQuery, terminate).block(TIMEOUT)));
        assertEquals(RdbErrorKind.CONNECTION, connectionError.kind(), () -> describe(connectionError));
        assertNotNull(connectionError.getCause());

        // 被杀掉的只是指定会话；新的连接仍应能立即完成轻查询。
        Number value = stableExecutor.query(SqlRequest.nativeSql("select 1 as FLYING_VALUE", List.of()))
                                     .single()
                                     .map(row -> (Number) rowValue(row, "FLYING_VALUE"))
                                     .block(TIMEOUT);
        assertNotNull(value);
        assertEquals(1, value.intValue());
    }

    private static Mono<Void> terminateSession(R2dbcSqlExecutor executor,
                                               long sessionId,
                                               SessionTermination termination) {
        return switch (termination) {
            case POSTGRESQL -> executor.query(SqlRequest.nativeSql(
                                              "select pg_terminate_backend(" + sessionId
                                                      + ") as FLYING_TERMINATED",
                                              List.of()))
                                      .then();
            case MYSQL -> executor.rowsUpdated(SqlRequest.nativeSql(
                                               "kill connection " + sessionId,
                                               List.of()))
                                   .then();
            case SQL_SERVER -> executor.rowsUpdated(SqlRequest.nativeSql("kill " + sessionId, List.of())).then();
            case ORACLE -> oracleSessionSerial(executor, sessionId)
                    .flatMap(serial -> executor.rowsUpdated(SqlRequest.nativeSql(
                                                            "alter system kill session '" + sessionId + ","
                                                                    + serial + "' immediate",
                                                            List.of())))
                    // ORA-00031 表示会话已经进入终止流程，认证目标已经达到；其他管理错误仍必须向外抛出。
                    .onErrorResume(error -> oracleSessionMarkedForKill(error) ? Mono.empty() : Mono.error(error))
                    .then();
        };
    }

    private static boolean oracleSessionMarkedForKill(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof R2dbcException r2dbc && r2dbc.getErrorCode() == 31) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static Mono<Long> oracleSessionSerial(R2dbcSqlExecutor executor, long sessionId) {
        return executor.query(SqlRequest.nativeSql(
                               "select serial# as FLYING_SERIAL from v$session where sid = " + sessionId,
                               List.of()))
                       .single()
                       .map(row -> toLong(rowValue(row, "FLYING_SERIAL")));
    }

    private static long readLong(Connection connection, String sql) {
        Object value = Flux.from(connection.createStatement(sql).execute())
                           .flatMap(result -> result.map((row, metadata) -> row.get(0)))
                           .single()
                           .block(TIMEOUT);
        assertNotNull(value);
        return toLong(value);
    }

    private static long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(value.toString());
    }

    private static long count(R2dbcSqlExecutor executor, String table) {
        Number value = executor.query(SqlRequest.nativeSql(
                                       "select count(*) as FLYING_COUNT from " + table,
                                       List.of()))
                               .single()
                               .map(row -> (Number) rowValue(row, "FLYING_COUNT"))
                               .block(TIMEOUT);
        assertNotNull(value);
        return value.longValue();
    }

    private static ConnectionFactory singleConnection(ConnectionFactory metadataSource, Connection connection) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.just(connection);
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return metadataSource.getMetadata();
            }
        };
    }

    private static Object rowValue(Map<String, Object> row, String name) {
        return row.entrySet()
                  .stream()
                  .filter(entry -> entry.getKey().equalsIgnoreCase(name))
                  .map(Map.Entry::getValue)
                  .findFirst()
                  .orElseThrow(() -> new AssertionError("missing column " + name + " in " + row.keySet()));
    }

    private static String describe(RdbException error) {
        Throwable cause = error.getCause();
        return "kind=" + error.kind()
                + ", sqlState=" + error.sqlState()
                + ", errorCode=" + error.errorCode()
                + ", cause=" + cause.getClass().getName()
                + ", causeMessage=" + cause.getMessage();
    }

    /**
     * 真实死锁失败时把两个竞争分支都展开。数据库可能把其中一个事务整体回滚，也可能由驱动再包一层异常；
     * 只有看到原始错误码和翻译结果，才能判断该补错误映射还是该调整造死锁的 SQL。
     */
    private static String describeLockAttempts(List<LockAttempt> attempts) {
        StringBuilder message = new StringBuilder("没有得到唯一 DEADLOCK，竞争分支=");
        for (int index = 0; index < attempts.size(); index++) {
            Throwable error = attempts.get(index).error();
            message.append(index).append('[').append(describeThrowable(error));
            if (error != null) {
                RuntimeException translated = RdbExceptionTranslator.translate(error);
                if (translated instanceof RdbException rdb) {
                    message.append(", translatedKind=").append(rdb.kind())
                           .append(", translatedState=").append(rdb.sqlState())
                           .append(", translatedCode=").append(rdb.errorCode());
                } else {
                    message.append(", translated=").append(describeThrowable(translated));
                }
            }
            message.append(']');
        }
        return message.toString();
    }

    private static String describeThrowable(Throwable error) {
        if (error == null) {
            return "success";
        }
        StringBuilder description = new StringBuilder(error.getClass().getName())
                .append(": ").append(error.getMessage());
        if (error instanceof R2dbcException r2dbc) {
            description.append(", sqlState=").append(r2dbc.getSqlState())
                       .append(", errorCode=").append(r2dbc.getErrorCode());
        }
        if (error.getCause() != null && error.getCause() != error) {
            description.append(", cause=").append(error.getCause().getClass().getName())
                       .append(": ").append(error.getCause().getMessage());
        }
        return description.toString();
    }

    private static Mono<LockAttempt> competingLock(Connection connection, String sql) {
        return lockRow(connection, sql)
                .thenReturn(LockAttempt.succeeded())
                .onErrorResume(error -> rollback(connection).thenReturn(LockAttempt.failed(error)));
    }

    private static Mono<Void> lockRow(Connection connection, String sql) {
        // UPDATE 只有在 Result 被消费后才真正执行，不能只创建 Statement 就认为写锁已经拿到。
        return Flux.from(connection.createStatement(sql).execute())
                   .flatMap(result -> result.getRowsUpdated())
                   .then();
    }

    private static Mono<Void> rollbackAndClose(Connection connection) {
        return rollback(connection)
                .timeout(Duration.ofSeconds(3))
                .onErrorResume(ignored -> Mono.empty())
                .then(Mono.from(connection.close())
                          .timeout(Duration.ofSeconds(3))
                          .onErrorResume(ignored -> Mono.empty()));
    }

    private static Mono<Void> rollback(Connection connection) {
        return Mono.from(connection.rollbackTransaction()).onErrorResume(ignored -> Mono.empty());
    }

    private static Mono<Long> cleanup(R2dbcSqlExecutor executor, String cleanupSql) {
        return executor.rowsUpdated(SqlRequest.nativeSql(cleanupSql, List.of()))
                       .onErrorResume(ignored -> Mono.empty());
    }

    private record LockAttempt(Throwable error) {

        static LockAttempt succeeded() {
            return new LockAttempt(null);
        }

        static LockAttempt failed(Throwable error) {
            return new LockAttempt(error);
        }
    }

    private enum SessionTermination {
        MYSQL(false),
        POSTGRESQL(false),
        ORACLE(true),
        SQL_SERVER(true);

        private final boolean slowStatementAsUpdate;

        SessionTermination(boolean slowStatementAsUpdate) {
            this.slowStatementAsUpdate = slowStatementAsUpdate;
        }

        private boolean executesSlowStatementAsUpdate() {
            return slowStatementAsUpdate;
        }
    }

}
