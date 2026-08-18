package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证同步入口走原生 JDBC，并保持参数顺序、紧凑结果、生成键和执行保护。 */
class JdbcSqlExecutorTest {

    /** 写 SQL 已进入驱动后超时、取消或结果未知，自有连接都不能再归还连接池。 */
    @Test
    void discardsOwnedConnectionAfterUncertainRegularWriteFailure() {
        List<SQLException> failures = List.of(
                new SQLTimeoutException("timed out", "HYT00"),
                new SQLException("cancelled", "HY008"),
                new SQLException("outcome unavailable"));
        List<RdbErrorKind> kinds = List.of(RdbErrorKind.TIMEOUT, RdbErrorKind.CANCELLED, RdbErrorKind.UNKNOWN);

        for (int index = 0; index < failures.size(); index++) {
            SQLException expected = failures.get(index);
            AtomicInteger aborts = new AtomicInteger();
            AtomicInteger closes = new AtomicInteger();
            PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
                if (method.getName().equals("executeLargeUpdate")) {
                    throw expected;
                }
                return defaultValue(method.getReturnType());
            });
            Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
                case "prepareStatement" -> statement;
                case "abort" -> {
                    aborts.incrementAndGet();
                    yield null;
                }
                case "close" -> {
                    closes.incrementAndGet();
                    yield null;
                }
                default -> defaultValue(method.getReturnType());
            });
            DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                    method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

            RdbException observed = assertThrows(RdbException.class, () -> JdbcSqlExecutor.create(dataSource)
                    .rowsUpdated(new SqlRequest("update device set name = ?", List.of("x"))));

            assertEquals(kinds.get(index), observed.kind());
            assertEquals(1, aborts.get());
            assertEquals(0, closes.get());
        }
    }

    /** 写入成功后读取生成键失败仍然是结果不确定，连接必须隔离。 */
    @Test
    void discardsOwnedConnectionWhenGeneratedKeyReadFailsAfterWrite() {
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> switch (
                method.getName()) {
            case "executeLargeUpdate" -> 1L;
            case "getGeneratedKeys" -> throw new SQLException("generated keys unavailable");
            default -> defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "prepareStatement" -> statement;
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        GeneratedKeyReadException observed = assertThrows(
                GeneratedKeyReadException.class, () -> JdbcSqlExecutor.create(dataSource)
                .rowsUpdatedReturningKeys(
                        new SqlRequest("insert into device(name) values (?)", List.of("x")),
                        SqlExecutionOptions.safeDefaults()));

        assertEquals(1L, observed.affectedRows());
        assertInstanceOf(SQLException.class, observed.getCause());
        assertEquals(1, aborts.get());
        assertEquals(0, closes.get());
    }

    /** 明确的 SQL 语法失败没有未知写入结果，连接仍可正常归池。 */
    @Test
    void returnsOwnedConnectionAfterClearRegularWriteFailure() {
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
            if (method.getName().equals("executeLargeUpdate")) {
                throw new SQLException("bad sql", "42000");
            }
            return defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "prepareStatement" -> statement;
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        RdbException observed = assertThrows(RdbException.class, () -> JdbcSqlExecutor.create(dataSource)
                .rowsUpdated(new SqlRequest("update missing set name = ?", List.of("x"))));

        assertEquals(RdbErrorKind.BAD_SQL, observed.kind());
        assertEquals(0, aborts.get());
        assertEquals(1, closes.get());
    }

    /** 业务密文和 CONTAINS 令牌必须在同一个自有 JDBC 事务中提交。 */
    @Test
    void commitsProtectedBusinessWriteAndContainsTokensAtomically() throws Exception {
        JdbcDataSource dataSource = dataSource("protected_atomic_success");
        createProtectedTables(dataSource);
        List<SqlExecutionObservation> observations = new ArrayList<>();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource).withObserver(observations::add);
        byte[] token = new byte[]{1, 2, 3};

        SqlWriteResult result = executor.atomicProtectedWrite(
                protectedInsertWork("insert into protected_customer(id, contact) values (?, ?)", token),
                SqlExecutionOptions.safeDefaults());

        assertEquals(1L, result.affectedRows());
        assertEquals(1L, count(executor, "select count(*) from protected_customer"));
        assertEquals(1L, count(executor, "select count(*) from protected_customer_tokens"));
        assertEquals(3, observations.size());
        SqlExecutionObservation protectedWrite = observations.getFirst();
        assertEquals(SqlExecutionOperation.UPDATE, protectedWrite.operation());
        assertEquals(SqlExecutionStatus.SUCCESS, protectedWrite.status());
        assertEquals("insert into protected_customer(id, contact) values (?, ?)", protectedWrite.sql());
        assertEquals(1L, protectedWrite.rows());
    }

    /** 侧索引写入失败必须回滚已经执行的业务写入，不能留下不可搜索的密文。 */
    @Test
    void rollsBackProtectedBusinessWriteWhenContainsTokenInsertFails() throws Exception {
        JdbcDataSource dataSource = dataSource("protected_atomic_rollback");
        createProtectedTables(dataSource);
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);
        ProtectedWriteWork valid = protectedInsertWork(
                "insert into protected_customer(id, contact) values (?, ?)", new byte[]{4, 5, 6});
        ProtectedWriteWork invalid = new ProtectedWriteWork(
                valid.kind(), valid.writeRequest(), valid.ownerQuery(), valid.ownerFields(), valid.knownOwner(),
                valid.ownerPredicateSql(),
                valid.deleteSql(), "insert into missing_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                valid.fields());

        assertThrows(RuntimeException.class,
                     () -> executor.atomicProtectedWrite(invalid, SqlExecutionOptions.safeDefaults()));

        assertEquals(0L, count(executor, "select count(*) from protected_customer"));
        assertEquals(0L, count(executor, "select count(*) from protected_customer_tokens"));
    }

    /** 侧索引 INSERT 被数据库静默忽略时也必须回滚，不能提交不可搜索的业务密文。 */
    @Test
    void rollsBackProtectedBusinessWriteWhenContainsTokenInsertAffectsNoRow() throws Exception {
        JdbcDataSource dataSource = dataSource("protected_atomic_zero_token_insert");
        createProtectedTables(dataSource);
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);
        ProtectedWriteWork valid = protectedInsertWork(
                "insert into protected_customer(id, contact) values (?, ?)", new byte[]{4, 5, 6});
        ProtectedWriteWork ignoredTokenInsert = new ProtectedWriteWork(
                valid.kind(), valid.writeRequest(), valid.ownerQuery(), valid.ownerFields(), valid.knownOwner(),
                valid.ownerPredicateSql(), valid.deleteSql(),
                "insert into protected_customer_tokens(id, field_tag, token_hash) "
                        + "select ?, ?, ? where 1 = 0",
                valid.fields());

        assertThrows(RuntimeException.class,
                     () -> executor.atomicProtectedWrite(
                             ignoredTokenInsert, SqlExecutionOptions.safeDefaults()));

        assertEquals(0L, count(executor, "select count(*) from protected_customer"));
        assertEquals(0L, count(executor, "select count(*) from protected_customer_tokens"));
    }

    /** 业务 SQL 已耗尽总截止时间后不能继续维护侧索引或提交，而要回滚整次保护写。 */
    @Test
    void rollsBackProtectedWriteBeforeSideIndexWhenTheTotalJdbcDeadlineExpires() {
        AtomicInteger preparedStatements = new AtomicInteger();
        AtomicInteger tokenExecutions = new AtomicInteger();
        AtomicInteger commits = new AtomicInteger();
        AtomicInteger rollbacks = new AtomicInteger();
        PreparedStatement business = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
            if (method.getName().equals("executeLargeUpdate")) {
                Thread.sleep(30L);
                return 1L;
            }
            return defaultValue(method.getReturnType());
        });
        PreparedStatement token = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
            if (method.getName().equals("executeUpdate")) {
                tokenExecutions.incrementAndGet();
                return 1;
            }
            return defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "getAutoCommit" -> true;
            case "prepareStatement" -> preparedStatements.getAndIncrement() == 0 ? business : token;
            case "commit" -> {
                commits.incrementAndGet();
                yield null;
            }
            case "rollback" -> {
                rollbacks.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        assertThrows(SqlExecutionTimeoutException.class, () -> JdbcSqlExecutor.create(dataSource)
                .atomicProtectedWrite(
                        protectedInsertWork("insert into protected_customer values (?, ?)", new byte[]{1}),
                        SqlExecutionOptions.timeout(Duration.ofMillis(10))));

        assertEquals(0, tokenExecutions.get());
        assertEquals(0, commits.get());
        assertEquals(1, rollbacks.get());
    }

    /** 数据库生成主键必须从业务 insert 的同一 Statement 读取并立即用于侧索引 owner。 */
    @Test
    void usesGeneratedOwnerKeyForProtectedContainsTokens() throws Exception {
        JdbcDataSource dataSource = dataSource("protected_generated_owner");
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table protected_customer("
                    + "id bigint generated by default as identity primary key, contact varbinary)");
            statement.execute("create table protected_customer_tokens("
                    + "id bigint not null, field_tag varchar(30) not null, token_hash varbinary not null)");
        }
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into protected_customer(contact) values (?)", List.of(new byte[]{7})),
                null, List.of("id"), Map.of(),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{8}))));

        SqlWriteResult result = executor.atomicProtectedWrite(work, SqlExecutionOptions.safeDefaults());

        assertEquals(1, result.generatedKeys().size());
        assertEquals(result.generatedKeys().getFirst().value(0),
                     executor.query(new SqlRequest("select id from protected_customer_tokens", List.of()))
                             .getFirst().value(0));
    }

    /** 显式空自增主键仍须由数据库生成键补齐，不能在侧索引 owner 解析时失败。 */
    @Test
    void replacesNullOwnerWithGeneratedKeyForProtectedContainsTokens() throws Exception {
        JdbcDataSource dataSource = dataSource("protected_null_generated_owner");
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table protected_customer("
                    + "id bigint generated by default as identity primary key, contact varbinary)");
            statement.execute("create table protected_customer_tokens("
                    + "id bigint not null, field_tag varchar(30) not null, token_hash varbinary not null)");
        }
        Map<String, Object> knownOwner = new LinkedHashMap<>();
        knownOwner.put("id", null);
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into protected_customer(contact) values (?)", List.of(new byte[]{7})),
                null, List.of("id"), knownOwner,
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{8}))));
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);

        SqlWriteResult result = executor.atomicProtectedWrite(work, SqlExecutionOptions.safeDefaults());

        assertEquals(result.generatedKeys().getFirst().value(0),
                     executor.query(new SqlRequest("select id from protected_customer_tokens", List.of()))
                             .getFirst().value(0));
    }

    /** 更新先锁定实际 owner，再在业务 update 成功后删除旧令牌并写入新令牌。 */
    @Test
    void replacesContainsTokensForTheOwnersSelectedInTheSameTransaction() throws Exception {
        JdbcDataSource dataSource = dataSource("protected_update_owner");
        createProtectedTables(dataSource);
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);
        executor.rowsUpdated(new SqlRequest(
                "insert into protected_customer(id, contact) values (?, ?)", List.of(1L, new byte[]{1})));
        executor.rowsUpdated(new SqlRequest(
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(1L, "contact", new byte[]{2})));
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPDATE,
                new SqlRequest("update protected_customer set contact = ? where id = ?",
                               List.of(new byte[]{3}, 1L)),
                new SqlRequest("select id from protected_customer where id = ?", List.of(1L)),
                List.of("id"), Map.of(),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{4}))));

        executor.atomicProtectedWrite(work, SqlExecutionOptions.safeDefaults());

        List<DynamicRow> tokens = executor.query(new SqlRequest(
                "select token_hash from protected_customer_tokens where id = ?", List.of(1L)));
        assertEquals(1, tokens.size());
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{4}, (byte[]) tokens.getFirst().value(0));
    }

    /** UPSERT 覆盖已有行时必须先删除旧 CONTAINS 令牌，避免旧明文仍可命中。 */
    @Test
    void replacesContainsTokensWhenProtectedUpsertUpdatesAnExistingOwner() throws Exception {
        JdbcDataSource dataSource = dataSource("protected_upsert_owner");
        createProtectedTables(dataSource);
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);
        executor.rowsUpdated(new SqlRequest(
                "insert into protected_customer(id, contact) values (?, ?)", List.of(1L, new byte[]{1})));
        executor.rowsUpdated(new SqlRequest(
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(1L, "contact", new byte[]{2})));
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPSERT,
                new SqlRequest("merge into protected_customer key(id) values (?, ?)",
                               List.of(1L, new byte[]{3})),
                null,
                List.of("id"), Map.of("id", 1L),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{4}))));

        executor.atomicProtectedWrite(work, SqlExecutionOptions.safeDefaults());

        List<DynamicRow> tokens = executor.query(new SqlRequest(
                "select token_hash from protected_customer_tokens where id = ?", List.of(1L)));
        assertEquals(1, tokens.size());
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{4}, (byte[]) tokens.getFirst().value(0));
    }

    /** 外部事务中的保护写只复用连接；提交、回滚和关闭仍由上层事务管理器负责。 */
    @Test
    void enlistsProtectedWriteWithoutCommittingTheExternalJdbcTransaction() throws Exception {
        JdbcDataSource dataSource = dataSource("protected_external_transaction");
        createProtectedTables(dataSource);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource)
                    .withTransactionParticipant(() -> Optional.of(
                            JdbcTransactionContext.external(connection, "primary")));

            executor.atomicProtectedWrite(
                    protectedInsertWork("insert into protected_customer(id, contact) values (?, ?)",
                                        new byte[]{6}),
                    SqlExecutionOptions.safeDefaults());
            connection.rollback();
        }

        JdbcSqlExecutor verifier = JdbcSqlExecutor.create(dataSource);
        assertEquals(0L, count(verifier, "select count(*) from protected_customer"));
        assertEquals(0L, count(verifier, "select count(*) from protected_customer_tokens"));
    }

    @Test
    void executesQueryUpdateAndGeneratedKeysWithoutReactiveBridge() throws Exception {
        JdbcDataSource dataSource = dataSource("native_path");
        createTable(dataSource);
        SyncSqlExecutor executor = SyncSqlExecutor.jdbc(dataSource);

        SqlWriteResult inserted = executor.rowsUpdatedReturningKeys(
                new SqlRequest("insert into device(name) values (?)", List.of("sensor-a")),
                SqlExecutionOptions.safeDefaults().withFetchSize(16));
        List<DynamicRow> rows = executor.query(
                new SqlRequest("select id, name from device where name = ?", List.of("sensor-a")));

        assertEquals(1L, inserted.affectedRows());
        assertEquals(1, inserted.generatedKeys().size());
        assertInstanceOf(Number.class, inserted.generatedKeys().getFirst().value(0));
        assertEquals(1, rows.size());
        assertEquals("sensor-a", rows.getFirst().get("NAME"));
    }

    /** JDBC 原生 SQL 在取得连接后按真实数据库词法校验，合法 MySQL 井号注释不能被二次拒绝。 */
    @Test
    void validatesNativeSqlWithJdbcDatabaseProduct() {
        java.util.concurrent.atomic.AtomicReference<String> preparedSql =
                new java.util.concurrent.atomic.AtomicReference<>();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
            if (method.getName().equals("executeLargeUpdate")) {
                return 1L;
            }
            return defaultValue(method.getReturnType());
        });
        java.sql.DatabaseMetaData metadata = proxy(java.sql.DatabaseMetaData.class,
                                                   (ignored, method, arguments) -> method.getName()
                                                           .equals("getDatabaseProductName")
                                                           ? "MySQL" : defaultValue(method.getReturnType()));
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> {
            if (method.getName().equals("getMetaData")) {
                return metadata;
            }
            if (method.getName().equals("prepareStatement")) {
                preparedSql.set((String) arguments[0]);
                return statement;
            }
            return defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));
        String sql = "update users set active = true # delete from audit_log\nwhere id = 1";

        long updated = JdbcSqlExecutor.create(dataSource).rowsUpdated(SqlRequest.nativeSql(sql, List.of()));

        assertEquals(1L, updated);
        assertEquals(sql, preparedSql.get());
    }

    /** 已取得真实数据库产品后，CANONICAL 请求也必须拒绝 SQL Server 的无分号批处理。 */
    @Test
    void validatesCanonicalSqlServerBatchWithJdbcDatabaseProduct() {
        java.util.concurrent.atomic.AtomicInteger prepareCalls = new java.util.concurrent.atomic.AtomicInteger();
        java.sql.DatabaseMetaData metadata = proxy(java.sql.DatabaseMetaData.class,
                                                   (ignored, method, arguments) -> method.getName()
                                                           .equals("getDatabaseProductName")
                                                           ? "Microsoft SQL Server"
                                                           : defaultValue(method.getReturnType()));
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> {
            if (method.getName().equals("getMetaData")) {
                return metadata;
            }
            if (method.getName().equals("prepareStatement")) {
                prepareCalls.incrementAndGet();
            }
            return defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        assertThrows(IllegalArgumentException.class,
                     () -> JdbcSqlExecutor.create(dataSource)
                             .rowsUpdated(new SqlRequest("select 1\nselect 2", List.of())));
        assertEquals(0, prepareCalls.get());
    }

    /** JDBC 原生生成键协作必须把明确列名交给驱动，Oracle 不得退化为 ROWID。 */
    @Test
    void requestsTheDeclaredGeneratedKeyColumnFromJdbcDriver() {
        java.util.concurrent.atomic.AtomicReference<String[]> requestedColumns =
                new java.util.concurrent.atomic.AtomicReference<>();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
            if (method.getName().equals("executeLargeUpdate")) {
                throw new SQLException("stop after statement preparation");
            }
            return defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> {
            if (method.getName().equals("prepareStatement") && arguments.length == 2
                    && arguments[1] instanceof String[] columns) {
                requestedColumns.set(columns.clone());
                return statement;
            }
            return defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        assertThrows(RuntimeException.class, () -> JdbcSqlExecutor.create(dataSource).rowsUpdatedReturningKeys(
                new SqlRequest("insert into device(name) values (?)", List.of("sensor")),
                SqlExecutionOptions.safeDefaults(),
                "id"));

        org.junit.jupiter.api.Assertions.assertArrayEquals(new String[]{"id"}, requestedColumns.get());
    }

    @Test
    void detectsTheFirstRowBeyondTheConfiguredLimit() throws Exception {
        JdbcDataSource dataSource = dataSource("row_limit");
        createTable(dataSource);
        SyncSqlExecutor executor = SyncSqlExecutor.jdbc(dataSource);
        executor.rowsUpdated(new SqlRequest("insert into device(name) values (?)", List.of("a")));
        executor.rowsUpdated(new SqlRequest("insert into device(name) values (?)", List.of("b")));

        SqlRowLimitExceededException error = assertThrows(
                SqlRowLimitExceededException.class,
                () -> executor.query(new SqlRequest("select id, name from device order by id", List.of()),
                                     SqlExecutionOptions.unlimited().withMaxRows(1)));

        assertEquals(1L, error.maxRows());
        assertEquals(1L, error.overflowIndex());
    }

    /** 验证已中断的同步查询不会再推进结果集，避免取消信号被一次潜在阻塞的 next() 吞掉。 */
    @Test
    void checksInterruptionBeforeAdvancingQueryResultSet() throws Exception {
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (ignored, method, arguments) -> switch (
                method.getName()) {
            case "getColumnCount" -> 1;
            case "getColumnLabel", "getColumnName" -> "value";
            default -> defaultValue(method.getReturnType());
        });
        ResultSet resultSet = proxy(ResultSet.class, (ignored, method, arguments) -> {
            if (method.getName().equals("next")) {
                nextCalls.incrementAndGet();
                return false;
            }
            if (method.getName().equals("getMetaData")) {
                return metadata;
            }
            return defaultValue(method.getReturnType());
        });
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
            if (method.getName().equals("cancel")) {
                cancellations.incrementAndGet();
            }
            return defaultValue(method.getReturnType());
        });

        Thread.currentThread().interrupt();
        try {
            SQLException error = assertThrows(SQLException.class, () -> JdbcResultSetReader.readQueryRows(
                    resultSet,
                    statement,
                    com.flying.orm.rdb.observation.SqlStatementType.SELECT,
                    SqlExecutionOptions.safeDefaults(),
                    new ArrayList<>()));

            assertEquals("HY008", error.getSQLState());
            assertEquals(0, nextCalls.get());
            assertEquals(1, cancellations.get());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void usesThePortableIntRowLimitBeforeTryingTheOptionalLongMethod() throws Exception {
        AtomicInteger intLimit = new AtomicInteger();
        AtomicInteger longCalls = new AtomicInteger();
        Statement statement = (Statement) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{Statement.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("setMaxRows")) intLimit.set((Integer) arguments[0]);
                    if (method.getName().equals("setLargeMaxRows")) longCalls.incrementAndGet();
                    return null;
                });

        JdbcStatementOptions.apply(statement, SqlExecutionOptions.safeDefaults());

        assertEquals(100_001, intLimit.get());
        assertEquals(0, longCalls.get());
    }

    @Test
    void emitsTheSharedObservationModel() throws Exception {
        JdbcDataSource dataSource = dataSource("observation");
        createTable(dataSource);
        List<SqlExecutionObservation> observations = new ArrayList<>();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource).withObserver(observations::add);

        executor.rowsUpdated(new SqlRequest("insert into device(name) values (?)", List.of("observed")));
        executor.query(new SqlRequest("select id from device", List.of()));

        assertEquals(2, observations.size());
        assertEquals(SqlExecutionBackend.JDBC, observations.getFirst().backend());
        assertEquals(SqlExecutionBackend.JDBC, observations.getLast().backend());
        assertEquals(1L, observations.get(0).rows());
        assertEquals(1L, observations.get(1).rows());
    }

    /** JDBC 观测旁路可隔离普通异常，但不能吞掉被适配器包装的 JVM 致命错误。 */
    @Test
    void propagatesVirtualMachineErrorNestedInSqlObserverFailure() throws Exception {
        JdbcDataSource dataSource = dataSource("observation_nested_fatal");
        createTable(dataSource);
        OutOfMemoryError fatal = new OutOfMemoryError("sql observer fatal");
        AtomicInteger callbacks = new AtomicInteger();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource).withObserver(ignored -> {
            if (callbacks.getAndIncrement() == 0) {
                throw new IllegalStateException("observer wrapper", fatal);
            }
        });

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, () -> executor.rowsUpdated(
                new SqlRequest("insert into device(name) values (?)", List.of("observed"))));

        assertSame(fatal, observed);
        assertEquals(1L, count(executor, "select count(*) from device"));
    }

    @Test
    void reusesExternalTransactionConnectionWithoutClosingIt() throws Exception {
        JdbcDataSource dataSource = dataSource("external_transaction");
        createTable(dataSource);
        List<SqlTransactionSource> sources = new ArrayList<>();
        SqlExecutionObserver observer = new SqlExecutionObserver() {
            @Override
            public void onExecution(SqlExecutionObservation observation) {
            }

            @Override
            public void onExecution(SqlExecutionObservation observation, SqlTransactionSource source) {
                sources.add(source);
            }

            @Override
            public boolean requiresTransactionSource() {
                return true;
            }
        };

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource)
                    .withObserver(observer)
                    .withTransactionParticipant(() -> Optional.of(
                            JdbcTransactionContext.external(connection, "primary")));

            executor.rowsUpdated(new SqlRequest("insert into device(name) values (?)", List.of("tx")));

            assertEquals(List.of(SqlTransactionSource.EXTERNAL), sources);
            assertEquals(false, connection.isClosed());
            connection.rollback();
        }
    }

    @Test
    void rejectsTransactionRouteChangesBeforeExecutingSql() throws Exception {
        JdbcDataSource dataSource = dataSource("transaction_route_change");
        createTable(dataSource);
        try (Connection connection = dataSource.getConnection()) {
            JdbcTransactionContext context = JdbcTransactionContext.external(connection, "primary");
            JdbcTransactionParticipant participant = new JdbcTransactionParticipant() {
                @Override
                public Optional<JdbcTransactionContext> currentTransaction() {
                    return Optional.of(context);
                }

                @Override
                public String currentRoutingIdentity() {
                    return "replica";
                }
            };
            JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource)
                    .withTransactionParticipant(participant);

            assertThrows(IllegalStateException.class, () -> executor.rowsUpdated(
                    new SqlRequest("insert into device(name) values (?)", List.of("wrong-route"))));
            assertEquals(0, countRows(connection));
        }
    }

    @Test
    void usesBoundExternalTransactionRouteForMetadataCachePartition() throws Exception {
        JdbcDataSource dataSource = dataSource("metadata_partition_from_transaction");
        try (Connection connection = dataSource.getConnection()) {
            JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource)
                    .withTransactionParticipant(() -> Optional.of(
                            JdbcTransactionContext.external(connection, "tenant-primary")));

            assertEquals("tenant-primary", executor.metadataCachePartition());
        }
    }

    @Test
    void rejectsConflictingRouteBeforeMetadataCacheCanUseIt() throws Exception {
        JdbcDataSource dataSource = dataSource("metadata_partition_route_conflict");
        try (Connection connection = dataSource.getConnection()) {
            JdbcTransactionContext context = JdbcTransactionContext.external(connection, "tenant-primary");
            JdbcTransactionParticipant participant = new JdbcTransactionParticipant() {
                @Override
                public Optional<JdbcTransactionContext> currentTransaction() {
                    return Optional.of(context);
                }

                @Override
                public String currentRoutingIdentity() {
                    return "tenant-replica";
                }
            };
            JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource)
                    .withTransactionParticipant(participant);

            assertThrows(IllegalStateException.class, executor::metadataCachePartition);
        }
    }

    @Test
    void preservesRequestedRouteForMetadataCacheWithoutExternalTransaction() {
        JdbcDataSource dataSource = dataSource("metadata_partition_without_transaction");
        JdbcTransactionParticipant participant = new JdbcTransactionParticipant() {
            @Override
            public Optional<JdbcTransactionContext> currentTransaction() {
                return Optional.empty();
            }

            @Override
            public String currentRoutingIdentity() {
                return "tenant-primary";
            }
        };
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource)
                .withTransactionParticipant(participant);

        assertEquals("tenant-primary", executor.metadataCachePartition());
    }

    @Test
    void invalidatesOwnedConnectionAfterJdbcConnectionFailure() {
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
            if (method.getName().equals("executeLargeUpdate")) {
                throw new SQLException("connection lost", "08006");
            }
            return defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "prepareStatement" -> statement;
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);

        assertThrows(RuntimeException.class, () -> executor.rowsUpdated(
                new SqlRequest("update device set name = ?", List.of("sensor"))));
        assertEquals(1, aborts.get());
        assertEquals(0, closes.get());
    }

    /** 驱动执行阶段包装的 JVM 致命错误必须先隔离自有连接，再按原对象传播。 */
    @Test
    void invalidatesOwnedConnectionAfterNestedVirtualMachineError() {
        OutOfMemoryError fatal = new OutOfMemoryError("fatal");
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
            if (method.getName().equals("executeLargeUpdate")) {
                throw new IllegalStateException("driver wrapper", fatal);
            }
            return defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "prepareStatement" -> statement;
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, () ->
                JdbcSqlExecutor.create(dataSource).rowsUpdated(
                        new SqlRequest("update device set name = ?", List.of("sensor"))));

        assertSame(fatal, observed);
        assertEquals(1, aborts.get());
        assertEquals(0, closes.get());
    }

    /** 外部事务连接即使遇到包装的 JVM 致命错误也仍由外部事务控制，ORM 不得关闭或失效它。 */
    @Test
    void preservesExternalTransactionOwnershipAfterNestedVirtualMachineError() {
        OutOfMemoryError fatal = new OutOfMemoryError("fatal");
        AtomicInteger acquisitions = new AtomicInteger();
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
            if (method.getName().equals("executeLargeUpdate")) {
                throw new IllegalStateException("driver wrapper", fatal);
            }
            return defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "prepareStatement" -> statement;
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) -> {
            if (method.getName().equals("getConnection")) {
                acquisitions.incrementAndGet();
                return connection;
            }
            return defaultValue(method.getReturnType());
        });
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource)
                .withTransactionParticipant(() -> Optional.of(
                        JdbcTransactionContext.external(connection, "primary")));

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, () -> executor.rowsUpdated(
                new SqlRequest("update device set name = ?", List.of("sensor"))));

        assertSame(fatal, observed);
        assertEquals(0, acquisitions.get());
        assertEquals(0, aborts.get());
        assertEquals(0, closes.get());
    }

    @Test
    void invalidatesOwnedConnectionWhenStatementCleanupReportsConnectionFailure() {
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
            if (method.getName().equals("close")) {
                throw new SQLException("connection lost while closing statement", "08006");
            }
            return defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "prepareStatement" -> statement;
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        long rows = JdbcSqlExecutor.create(dataSource).rowsUpdated(
                new SqlRequest("update device set name = ?", List.of("sensor")));

        assertEquals(0L, rows);
        assertEquals(1, aborts.get());
        assertEquals(0, closes.get());
    }

    /** 受保护写入提交已经确认后，连接归还失败只能作为资源清理故障，不能推翻业务结果。 */
    @Test
    void keepsCommittedProtectedWriteResultWhenOwnedConnectionCloseFails() {
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> switch (
                method.getName()) {
            case "executeLargeUpdate" -> 1L;
            case "executeUpdate" -> 1;
            default -> defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "getAutoCommit" -> false;
            case "prepareStatement" -> statement;
            case "commit" -> null;
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                throw new SQLException("connection close failed", "08006");
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        SqlWriteResult result = JdbcSqlExecutor.create(dataSource)
                .atomicProtectedWrite(
                        protectedInsertWork("insert into protected_customer(id, contact) values (?, ?)",
                                            new byte[]{1}),
                        SqlExecutionOptions.safeDefaults());

        assertEquals(1L, result.affectedRows());
        assertEquals(1, closes.get());
        assertEquals(1, aborts.get());
    }

    /** 外部事务中的业务 INSERT 已执行后，生成键读取失败必须保留 ENLISTED 所需的写入证据。 */
    @Test
    void preservesGeneratedKeyReadEvidenceForExternalProtectedWrite() {
        AtomicInteger commits = new AtomicInteger();
        AtomicInteger rollbacks = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> switch (
                method.getName()) {
            case "executeLargeUpdate" -> 1L;
            case "getGeneratedKeys" -> throw new SQLException("generated keys unavailable");
            default -> defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "prepareStatement" -> statement;
            case "commit" -> {
                commits.incrementAndGet();
                yield null;
            }
            case "rollback" -> {
                rollbacks.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));
        JdbcTransactionParticipant participant = () -> Optional.of(
                JdbcTransactionContext.external(connection, "primary"));

        GeneratedKeyReadException observed = assertThrows(
                GeneratedKeyReadException.class,
                () -> JdbcSqlExecutor.create(dataSource)
                                     .withTransactionParticipant(participant)
                                     .atomicProtectedWrite(
                                             protectedGeneratedInsertWork(), SqlExecutionOptions.safeDefaults()));

        assertEquals(1L, observed.affectedRows());
        assertInstanceOf(SQLException.class, observed.getCause());
        assertEquals(0, commits.get());
        assertEquals(0, rollbacks.get());
        assertEquals(0, closes.get());
    }

    /** 自有事务确认回滚后写入已撤销，不能继续把生成键读取失败报告成未确认写入。 */
    @Test
    void doesNotReportUnknownWhenProtectedGeneratedKeyFailureWasRolledBack() {
        AtomicInteger rollbacks = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> switch (
                method.getName()) {
            case "executeLargeUpdate" -> 1L;
            case "getGeneratedKeys" -> throw new SQLException("generated keys unavailable");
            default -> defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "getAutoCommit" -> false;
            case "prepareStatement" -> statement;
            case "rollback" -> {
                rollbacks.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        RdbException observed = assertThrows(
                RdbException.class,
                () -> JdbcSqlExecutor.create(dataSource).atomicProtectedWrite(
                        protectedGeneratedInsertWork(), SqlExecutionOptions.safeDefaults()));

        assertEquals(1, rollbacks.get());
        assertInstanceOf(SQLException.class, observed.getCause());
    }

    /** 生成键读取失败后的回滚结果未知时，必须保留 affected rows 供 Repository 报告 UNKNOWN。 */
    @Test
    void preservesGeneratedKeyReadEvidenceWhenProtectedRollbackIsUnknown() {
        AtomicInteger aborts = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> switch (
                method.getName()) {
            case "executeLargeUpdate" -> 1L;
            case "getGeneratedKeys" -> throw new SQLException("generated keys unavailable");
            default -> defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "getAutoCommit" -> false;
            case "prepareStatement" -> statement;
            case "rollback" -> throw new SQLException("rollback reply lost", "08006");
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        GeneratedKeyReadException observed = assertThrows(
                GeneratedKeyReadException.class,
                () -> JdbcSqlExecutor.create(dataSource).atomicProtectedWrite(
                        protectedGeneratedInsertWork(), SqlExecutionOptions.safeDefaults()));

        assertEquals(1L, observed.affectedRows());
        RdbException unknown = assertInstanceOf(RdbException.class, observed.getCause());
        assertEquals(RdbErrorKind.UNKNOWN, unknown.kind());
        assertEquals(1, aborts.get());
    }

    /** 提交已经确认后，恢复 auto-commit 的普通故障只能隔离连接并进入清理观测，不能把成功改写成失败。 */
    @Test
    void keepsCommittedProtectedWriteResultWhenAutoCommitRestoreFails() {
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        List<ResourceCleanupObservation> cleanup = new ArrayList<>();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> switch (
                method.getName()) {
            case "executeLargeUpdate" -> 1L;
            case "executeUpdate" -> 1;
            default -> defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "getAutoCommit" -> true;
            case "setAutoCommit" -> {
                if (Boolean.TRUE.equals(arguments[0])) {
                    throw new SQLException("auto-commit restore failed", "08006");
                }
                yield null;
            }
            case "prepareStatement" -> statement;
            case "commit" -> null;
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));
        SqlExecutionObserver observer = new SqlExecutionObserver() {
            @Override
            public void onExecution(SqlExecutionObservation observation) {
            }

            @Override
            public void onResourceCleanup(ResourceCleanupObservation observation) {
                cleanup.add(observation);
            }
        };

        SqlWriteResult result = JdbcSqlExecutor.create(dataSource)
                .withObserver(observer)
                .atomicProtectedWrite(
                        protectedInsertWork("insert into protected_customer(id, contact) values (?, ?)",
                                            new byte[]{1}),
                        SqlExecutionOptions.safeDefaults());

        assertEquals(1L, result.affectedRows());
        assertEquals(1, aborts.get());
        assertEquals(0, closes.get());
        assertEquals(1, cleanup.size());
        assertEquals(ResourceCleanupObservation.Phase.SESSION_CLEANUP, cleanup.getFirst().phase());
        assertTrue(cleanup.getFirst().outcomeConfirmed());
    }

    /** 自有保护写无法读取事务状态时必须先尝试回滚并隔离连接，不能按普通连接归还。 */
    @Test
    void rollsBackAndInvalidatesProtectedWriteWhenAutoCommitStateCannotBeRead() {
        AtomicInteger rollbacks = new AtomicInteger();
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "getAutoCommit" -> throw new SQLException("connection state unavailable", "08006");
            case "rollback" -> {
                rollbacks.incrementAndGet();
                yield null;
            }
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        RdbException observed = assertThrows(RdbException.class, () -> JdbcSqlExecutor.create(dataSource)
                .atomicProtectedWrite(
                        protectedInsertWork("insert into protected_customer values (?, ?)", new byte[]{1}),
                        SqlExecutionOptions.safeDefaults()));

        assertEquals(RdbErrorKind.CONNECTION, observed.kind());
        assertEquals(1, rollbacks.get());
        assertEquals(1, aborts.get());
        assertEquals(0, closes.get());
    }

    /** 保护写提交失败包装的 VME 必须在隔离未知事务连接后原样传播。 */
    @Test
    void promotesVirtualMachineErrorNestedInProtectedWriteCommitFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("fatal");
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> switch (
                method.getName()) {
            case "executeLargeUpdate" -> 1L;
            case "executeUpdate" -> 1;
            default -> defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "getAutoCommit" -> false;
            case "prepareStatement" -> statement;
            case "commit" -> throw new IllegalStateException("driver wrapper", fatal);
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        Throwable observed = assertThrows(Throwable.class, () -> JdbcSqlExecutor.create(dataSource)
                .atomicProtectedWrite(
                        protectedInsertWork("insert into protected_customer(id, contact) values (?, ?)",
                                            new byte[]{1}),
                        SqlExecutionOptions.safeDefaults()));

        assertSame(fatal, observed);
        assertEquals(1, aborts.get());
        assertEquals(0, closes.get());
    }

    /** 提交回执丢失后连接已经不可判定，不能再用 setAutoCommit(true) 隐式改变事务结果。 */
    @Test
    void doesNotRestoreAutoCommitAfterProtectedWriteCommitOutcomeBecomesUnknown() {
        AtomicInteger restores = new AtomicInteger();
        AtomicInteger rollbacks = new AtomicInteger();
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> switch (
                method.getName()) {
            case "executeLargeUpdate" -> 1L;
            case "executeUpdate" -> 1;
            default -> defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "getAutoCommit" -> true;
            case "setAutoCommit" -> {
                if (Boolean.TRUE.equals(arguments[0])) {
                    restores.incrementAndGet();
                }
                yield null;
            }
            case "prepareStatement" -> statement;
            case "commit" -> throw new SQLException("commit reply lost", "08006");
            case "rollback" -> {
                rollbacks.incrementAndGet();
                yield null;
            }
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        RdbException observed = assertThrows(RdbException.class, () -> JdbcSqlExecutor.create(dataSource)
                .atomicProtectedWrite(
                        protectedInsertWork("insert into protected_customer(id, contact) values (?, ?)",
                                            new byte[]{1}),
                        SqlExecutionOptions.safeDefaults()));

        assertEquals(RdbErrorKind.UNKNOWN, observed.kind());
        assertEquals(0, restores.get());
        assertEquals(0, rollbacks.get());
        assertEquals(1, aborts.get());
        assertEquals(0, closes.get());
    }

    /** 业务失败后的回滚回执丢失时必须报告 UNKNOWN，不能降级成普通 SQL 失败。 */
    @Test
    void reportsUnknownWhenProtectedWriteRollbackCannotBeConfirmed() {
        SQLException operation = new SQLException("write failed", "42000");
        SQLException rollback = new SQLException("rollback reply lost", "08006");
        AtomicInteger restores = new AtomicInteger();
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
            if (method.getName().equals("executeLargeUpdate")) {
                throw operation;
            }
            return defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "getAutoCommit" -> true;
            case "prepareStatement" -> statement;
            case "rollback" -> throw rollback;
            case "setAutoCommit" -> {
                if (Boolean.TRUE.equals(arguments[0])) {
                    restores.incrementAndGet();
                }
                yield null;
            }
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        RdbException observed = assertThrows(RdbException.class, () -> JdbcSqlExecutor.create(dataSource)
                .atomicProtectedWrite(protectedInsertWork("insert into protected_customer values (?, ?)",
                                                          new byte[]{1}),
                                      SqlExecutionOptions.safeDefaults()));

        assertEquals(RdbErrorKind.UNKNOWN, observed.kind());
        assertSame(operation, observed.getCause());
        assertTrue(List.of(operation.getSuppressed()).contains(rollback));
        assertEquals(0, restores.get());
        assertEquals(1, aborts.get());
        assertEquals(0, closes.get());
    }

    /** 业务致命错误必须主导后续 auto-commit 恢复致命错误，不能被 finally 覆盖。 */
    @Test
    void retainsProtectedWritePrimaryFatalWhenAutoCommitRestoreAlsoFailsFatally() {
        OutOfMemoryError primary = new OutOfMemoryError("primary");
        OutOfMemoryError restore = new OutOfMemoryError("restore");
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
            if (method.getName().equals("executeLargeUpdate")) {
                throw primary;
            }
            return defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "getAutoCommit" -> true;
            case "prepareStatement" -> statement;
            case "setAutoCommit" -> {
                if (Boolean.TRUE.equals(arguments[0])) {
                    throw restore;
                }
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        Throwable observed = assertThrows(Throwable.class, () -> JdbcSqlExecutor.create(dataSource)
                .atomicProtectedWrite(protectedInsertWork("insert into protected_customer values (?, ?)",
                                                          new byte[]{1}),
                                      SqlExecutionOptions.safeDefaults()));

        assertSame(primary, observed);
        assertTrue(List.of(primary.getSuppressed()).contains(restore));
    }

    /** 回滚结果无法确认时必须隔离连接，不能再恢复 auto-commit 改变未知事务。 */
    @Test
    void doesNotRestoreAutoCommitAfterProtectedWriteRollbackFailsFatally() {
        AssertionError operation = new AssertionError("operation");
        OutOfMemoryError rollback = new OutOfMemoryError("rollback");
        AtomicInteger restores = new AtomicInteger();
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        PreparedStatement statement = proxy(PreparedStatement.class, (ignored, method, arguments) -> {
            if (method.getName().equals("executeLargeUpdate")) {
                throw operation;
            }
            return defaultValue(method.getReturnType());
        });
        Connection connection = proxy(Connection.class, (ignored, method, arguments) -> switch (method.getName()) {
            case "getAutoCommit" -> true;
            case "prepareStatement" -> statement;
            case "rollback" -> throw rollback;
            case "setAutoCommit" -> {
                if (Boolean.TRUE.equals(arguments[0])) {
                    restores.incrementAndGet();
                }
                yield null;
            }
            case "abort" -> {
                aborts.incrementAndGet();
                yield null;
            }
            case "close" -> {
                closes.incrementAndGet();
                yield null;
            }
            default -> defaultValue(method.getReturnType());
        });
        DataSource dataSource = proxy(DataSource.class, (ignored, method, arguments) ->
                method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));

        Throwable observed = assertThrows(Throwable.class, () -> JdbcSqlExecutor.create(dataSource)
                .atomicProtectedWrite(protectedInsertWork("insert into protected_customer values (?, ?)",
                                                          new byte[]{1}),
                                      SqlExecutionOptions.safeDefaults()));

        assertSame(rollback, observed);
        assertEquals(0, restores.get());
        assertEquals(1, aborts.get());
        assertEquals(0, closes.get());
    }

    private static JdbcDataSource dataSource(String name) {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        return dataSource;
    }

    private static ProtectedWriteWork protectedInsertWork(String sql, byte[] token) {
        return new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest(sql, List.of(1L, new byte[]{9, 8, 7})),
                null,
                List.of("id"),
                Map.of("id", 1L),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(token))));
    }

    private static ProtectedWriteWork protectedGeneratedInsertWork() {
        return new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into protected_customer(contact) values (?)", List.of(new byte[]{9, 8, 7})),
                null,
                List.of("id"),
                Map.of(),
                "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1}))));
    }

    private static void createProtectedTables(JdbcDataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table protected_customer(id bigint primary key, contact varbinary)");
            statement.execute("create table protected_customer_tokens("
                    + "id bigint not null, field_tag varchar(30) not null, token_hash varbinary not null)");
        }
    }

    private static long count(JdbcSqlExecutor executor, String sql) {
        return ((Number) executor.query(new SqlRequest(sql, List.of())).getFirst().value(0)).longValue();
    }

    private static void createTable(JdbcDataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table device (id bigint generated by default as identity primary key, "
                    + "name varchar(128) not null)");
        }
    }

    private static long countRows(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement();
             java.sql.ResultSet result = statement.executeQuery("select count(*) from device")) {
            result.next();
            return result.getLong(1);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private static Object defaultValue(Class<?> type) {
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        return null;
    }
}
