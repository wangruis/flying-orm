package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.JdbcTransactionCompletion;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;
import com.flying.orm.rdb.transaction.TransactionOutcome;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 只验证同步 Schema 的入口、事务守卫、执行结果和缓存失效边界。 */
class JdbcSchemaClientTest {

    @Test
    void createsTableThroughNativeSyncExecutor() {
        RecordingExecutor executor = new RecordingExecutor();
        List<String> invalidated = new ArrayList<>();
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.h2())
                                                   .withMetadataInvalidator(invalidated::add);

        assertEquals(1L, client.createTable(form()));
        assertEquals(List.of("create table Users (id BIGINT primary key, name VARCHAR)"), executor.sqlTexts());
        assertEquals(List.of("Users"), invalidated);
    }

    /** JDBC 建立受保护表单时同时创建侧索引表，两个物理表的 metadata 都必须精确失效。 */
    @Test
    void invalidatesPrimaryAndContainsSideMetadataAfterJdbcDdl() {
        RecordingExecutor executor = new RecordingExecutor();
        List<String> invalidated = new ArrayList<>();
        DynamicForm form = containsProtectedForm();
        String sideTable = ProtectedContainsLayout.resolve(form).orElseThrow().table().table();
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.h2())
                                                   .withMetadataInvalidator(invalidated::add);

        client.createTable(form);

        assertEquals(List.of(form.table(), sideTable), invalidated);
    }

    /** JDBC 自动迁移必须清理参与规划的 reader 自身缓存，不能只通知额外回调。 */
    @Test
    void invalidatesPlanningReaderMetadataAfterJdbcAutomaticDdl() {
        RecordingExecutor ddlExecutor = new RecordingExecutor();
        MetadataExecutor metadataExecutor = new MetadataExecutor();
        List<String> readerInvalidated = new ArrayList<>();
        MetadataCacheInvalidator dependent = new MetadataCacheInvalidator() {
            @Override
            public void invalidate(String table) {
                readerInvalidated.add(table);
            }

            @Override
            public void invalidateAll() {
                throw new AssertionError("automatic DDL must use precise table invalidation");
            }
        };
        JdbcFormMetadataReader reader = JdbcFormMetadataReaders.cached(
                metadataExecutor, RdbDialect.h2(), CacheRegionPolicy.metadataDefaults(),
                dependent);
        JdbcSchemaClient client = JdbcSchemaClient.create(ddlExecutor, RdbDialect.h2());
        DynamicForm form = containsProtectedForm();
        String sideTable = ProtectedContainsLayout.resolve(form).orElseThrow().table().table();

        client.createOrAlterDetailed(form, List.of(), reader);
        reader.readTable(form.table());
        reader.readTable(sideTable);

        assertEquals(List.of(form.table(), sideTable), readerInvalidated);
        assertEquals(12, metadataExecutor.queryCalls.get());
    }

    /** DDL 已完成后的普通缓存回调故障可隔离，但其中包装的 JVM 致命错误必须原样传播。 */
    @Test
    void propagatesVirtualMachineErrorNestedInMetadataInvalidationFailure() {
        RecordingExecutor executor = new RecordingExecutor();
        OutOfMemoryError fatal = new OutOfMemoryError("metadata invalidation fatal");
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.h2())
                                                   .withMetadataInvalidator(ignored -> {
                                                       throw new IllegalStateException(
                                                               "invalidator wrapper", fatal);
                                                   });

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, () -> client.createTable(form()));

        assertSame(fatal, observed);
        assertEquals(List.of("create table Users (id BIGINT primary key, name VARCHAR)"), executor.sqlTexts());
    }

    @Test
    void invalidatesTargetTableAfterExplicitMigration() {
        RecordingExecutor executor = new RecordingExecutor();
        List<String> invalidated = new ArrayList<>();
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.h2())
                                                   .withMetadataInvalidator(invalidated::add);
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "VARCHAR"))
                                        .addField(DynamicField.of("email", "VARCHAR"))
                                        .build();

        assertEquals(1L, client.migrate(form().diffTo(target)));

        assertEquals(List.of("Users"), invalidated);
    }

    /** JDBC 迁移汇总不能把已确认的影响行数饱和或回绕。 */
    @Test
    void rejectsAffectedRowCountOverflowInsteadOfSaturatingJdbcSchemaResult() {
        RecordingExecutor executor = new RecordingExecutor();
        executor.withAffectedRows(Long.MAX_VALUE, 1L);
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.h2());
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("email", "VARCHAR"))
                                        .build();

        RdbException error = assertThrows(RdbException.class, () -> client.migrate(form().diffTo(target)));

        assertEquals(RdbErrorKind.UNKNOWN, error.kind());
        assertInstanceOf(ArithmeticException.class, error.getCause());
    }

    @Test
    void rejectsImplicitCommitDdlBeforeUsingExternalTransaction() {
        RecordingExecutor executor = new RecordingExecutor();
        JdbcTransactionParticipant participant = () -> Optional.of(
                JdbcTransactionContext.external(connectionProxy(), "primary"));
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.mysql(), participant);

        SchemaMigrationRejectedException error = assertThrows(
                SchemaMigrationRejectedException.class, () -> client.createTable(form()));

        assertEquals(SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED, error.failureCode());
        assertEquals(List.of(), executor.sqlTexts());
    }

    /** JDBC 外部事务 DDL 在 SQL 后立即失效，并在真实回滚后再次清掉可能重载的未提交结构。 */
    @Test
    void invalidatesJdbcMetadataAgainAfterExternalDdlTransactionCompletes() {
        RecordingExecutor executor = new RecordingExecutor();
        RecordingJdbcCompletion completion = new RecordingJdbcCompletion();
        JdbcTransactionParticipant participant = () -> Optional.of(
                JdbcTransactionContext.external(connectionProxy(), "primary", completion));
        List<String> invalidated = new ArrayList<>();
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.postgresql(), participant)
                                                   .withMetadataInvalidator(invalidated::add);

        assertEquals(1L, client.createTable(form()));
        assertEquals(List.of("Users"), invalidated);

        completion.complete(TransactionOutcome.ROLLED_BACK);

        assertEquals(List.of("Users", "Users"), invalidated);
    }

    /** JDBC 事务适配器没有完成通知时，事务型 DDL 也必须在第一条 SQL 前失败关闭。 */
    @Test
    void rejectsExternalJdbcDdlWithoutTransactionCompletionNotification() {
        RecordingExecutor executor = new RecordingExecutor();
        JdbcTransactionParticipant participant = () -> Optional.of(
                JdbcTransactionContext.external(connectionProxy(), "primary"));
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.postgresql(), participant);

        SchemaMigrationRejectedException error = assertThrows(
                SchemaMigrationRejectedException.class, () -> client.createTable(form()));

        assertEquals(SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED, error.failureCode());
        assertEquals(List.of(), executor.sqlTexts());
    }

    @Test
    void executesReviewedPlanAndInvalidatesAfterNativeExecution() {
        RecordingExecutor executor = new RecordingExecutor();
        List<String> invalidated = new ArrayList<>();
        List<SchemaMigrationObservation> observations = new ArrayList<>();
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.postgresql())
                                                   .withMetadataInvalidator(invalidated::add)
                                                   .withMigrationObserver(observations::add);
        SqlRequest request = new SqlRequest("alter table Users add column email VARCHAR", List.of());
        SchemaMigrationPlan migration = new SchemaMigrationPlan(
                form(), List.of(), true, List.of(request), List.of());
        ReviewedSchemaMigrationPlan reviewed = new ReviewedSchemaMigrationPlan(
                migration,
                new SchemaRollbackPlan(List.of(), List.of()),
                new OnlineDdlReview(OnlineDdlMode.ALLOW_BLOCKING, List.of()));

        SchemaMigrationResult result = client.executeReviewed(reviewed);

        assertEquals(1L, result.rowsUpdated());
        assertEquals(1, result.steps().size());
        assertEquals(List.of("Users"), invalidated);
        assertEquals(SqlExecutionStatus.SUCCESS, observations.getFirst().status());
    }

    @Test
    void rejectsExplicitLockTimeoutWithoutOneExternalSession() {
        RecordingExecutor executor = new RecordingExecutor();
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.postgresql())
                                                   .withDefaultMigrationExecutionOptions(
                                                           SchemaMigrationExecutionOptions.defaults());
        SqlRequest request = new SqlRequest("alter table Users add column email VARCHAR", List.of());
        ReviewedSchemaMigrationPlan reviewed = reviewedPlan(request);

        SchemaMigrationRejectedException error = assertThrows(
                SchemaMigrationRejectedException.class, () -> client.executeReviewed(reviewed));

        assertEquals(SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED, error.failureCode());
        assertEquals(List.of(), executor.sqlTexts());
    }

    /** work 与 cleanup 同时失败时必须保留两条异常链，且已开始的迁移仍触发精确缓存失效。 */
    @Test
    void preservesWorkFailureWhenJdbcSessionCleanupAlsoFails() {
        RuntimeException workFailure = new IllegalStateException("work failed");
        RuntimeException cleanupFailure = new IllegalStateException("cleanup failed");
        DualFailureExecutor executor = new DualFailureExecutor(
                "alter table Users add column email VARCHAR", workFailure, cleanupFailure);
        List<String> invalidated = new ArrayList<>();
        JdbcTransactionParticipant participant = () -> Optional.of(
                JdbcTransactionContext.external(connectionProxy(), "primary", new RecordingJdbcCompletion()));
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.postgresql(), participant)
                                                   .withMetadataInvalidator(invalidated::add)
                                                   .withDefaultMigrationExecutionOptions(
                                                           SchemaMigrationExecutionOptions.defaults());
        SqlRequest work = new SqlRequest("alter table Users add column email VARCHAR", List.of());

        SqlExecutionSequenceException error = assertThrows(
                SqlExecutionSequenceException.class, () -> client.executeReviewed(reviewedPlan(work)));

        assertEquals(SqlExecutionPhase.CLEANUP, error.phase());
        assertSame(cleanupFailure, error.getCause());
        assertEquals(1, error.getSuppressed().length);
        SqlExecutionSequenceException workSequence = assertInstanceOf(
                SqlExecutionSequenceException.class, error.getSuppressed()[0]);
        assertEquals(SqlExecutionPhase.WORK, workSequence.phase());
        assertSame(workFailure, workSequence.getCause());
        assertEquals(List.of("Users"), invalidated);
    }

    /** JDBC 会话 cleanup 必须使用 cleanupTimeout，不能继续沿用业务 DDL 的执行时限。 */
    @Test
    void usesCleanupTimeoutForJdbcSessionCleanupPhase() {
        RecordingExecutor executor = new RecordingExecutor();
        JdbcTransactionParticipant participant = () -> Optional.of(
                JdbcTransactionContext.external(connectionProxy(), "primary", new RecordingJdbcCompletion()));
        SqlExecutionOptions sqlOptions = SqlExecutionOptions.unlimited()
                                                         .withCleanupTimeout(Duration.ofSeconds(17));
        SchemaMigrationExecutionOptions migrationOptions = new SchemaMigrationExecutionOptions(
                sqlOptions, null, Duration.ofSeconds(1));
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.postgresql(), participant)
                                                   .withDefaultMigrationExecutionOptions(migrationOptions);

        client.executeReviewed(reviewedPlan(
                new SqlRequest("alter table Users add column email VARCHAR", List.of())));

        assertEquals(3, executor.options().size());
        assertEquals(Duration.ZERO, executor.options().get(0).timeout());
        assertEquals(Duration.ZERO, executor.options().get(1).timeout());
        assertEquals(Duration.ofSeconds(17), executor.options().get(2).timeout());
    }

    /** JDBC DDL work 直接抛出 VM 错误时，已进入的 lock-timeout 会话仍必须先 cleanup。 */
    @Test
    void cleansUpJdbcSessionBeforeRethrowingVirtualMachineErrorFromWork() {
        OutOfMemoryError fatal = new OutOfMemoryError("ddl work fatal");
        FatalWorkExecutor executor = new FatalWorkExecutor("alter table Users add column email VARCHAR", fatal);
        JdbcTransactionParticipant participant = () -> Optional.of(
                JdbcTransactionContext.external(connectionProxy(), "primary", new RecordingJdbcCompletion()));
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.postgresql(), participant)
                                                   .withDefaultMigrationExecutionOptions(
                                                           SchemaMigrationExecutionOptions.defaults());
        AtomicReference<OutOfMemoryError> observed = new AtomicReference<>();

        try {
            client.executeReviewed(reviewedPlan(new SqlRequest("alter table Users add column email VARCHAR", List.of())));
        } catch (OutOfMemoryError error) {
            observed.set(error);
        }

        assertSame(fatal, observed.get());
        assertEquals(1, executor.cleanupCalls());
    }

    /** 驱动适配器用 RuntimeException 包装 VME 时仍必须先恢复会话，再传播原始 fatal。 */
    @Test
    void cleansUpJdbcSessionBeforeRethrowingNestedVirtualMachineErrorFromWork() {
        OutOfMemoryError fatal = new OutOfMemoryError("nested DDL work fatal");
        FatalWorkExecutor executor = new FatalWorkExecutor(
                "alter table Users add column email VARCHAR",
                new IllegalStateException("driver wrapper", fatal));
        JdbcTransactionParticipant participant = () -> Optional.of(
                JdbcTransactionContext.external(connectionProxy(), "primary", new RecordingJdbcCompletion()));
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.postgresql(), participant)
                                                   .withDefaultMigrationExecutionOptions(
                                                           SchemaMigrationExecutionOptions.defaults());

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, () -> client.executeReviewed(
                reviewedPlan(new SqlRequest("alter table Users add column email VARCHAR", List.of()))));

        assertSame(fatal, observed);
        assertEquals(1, executor.cleanupCalls());
    }

    /** cleanup 的 Runtime 包装层同样不能把 JVM 致命错误降级为普通迁移序列失败。 */
    @Test
    void promotesVirtualMachineErrorNestedInJdbcSessionCleanupFailure() {
        RuntimeException workFailure = new IllegalStateException("work failed");
        OutOfMemoryError fatal = new OutOfMemoryError("nested DDL cleanup fatal");
        DualFailureExecutor executor = new DualFailureExecutor(
                "alter table Users add column email VARCHAR",
                workFailure,
                new IllegalStateException("cleanup wrapper", fatal));
        JdbcTransactionParticipant participant = () -> Optional.of(
                JdbcTransactionContext.external(connectionProxy(), "primary", new RecordingJdbcCompletion()));
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, RdbDialect.postgresql(), participant)
                                                   .withDefaultMigrationExecutionOptions(
                                                           SchemaMigrationExecutionOptions.defaults());

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, () -> client.executeReviewed(
                reviewedPlan(new SqlRequest("alter table Users add column email VARCHAR", List.of()))));

        assertSame(fatal, observed);
    }

    private static ReviewedSchemaMigrationPlan reviewedPlan(SqlRequest request) {
        return new ReviewedSchemaMigrationPlan(
                new SchemaMigrationPlan(form(), List.of(), true, List.of(request), List.of()),
                new SchemaRollbackPlan(List.of(), List.of()),
                new OnlineDdlReview(OnlineDdlMode.ALLOW_BLOCKING, List.of()));
    }

    private static final class RecordingJdbcCompletion implements JdbcTransactionCompletion {

        private Listener listener;

        @Override
        public boolean register(Listener listener) {
            this.listener = listener;
            return true;
        }

        private void complete(TransactionOutcome outcome) {
            Mono.from(listener.afterCompletion(outcome)).block();
        }
    }

    private static DynamicForm containsProtectedForm() {
        return DynamicForm.builder("protected-customer", "protected_customer")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("contact", "VARCHAR"))
                          .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                         .searchModes(
                                                                                 EncryptedSearchMode.CONTAINS)
                                                                         .build())
                          .build();
    }

    private static DynamicForm form() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .build();
    }

    private static Connection connectionProxy() {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> null);
    }

    private static final class RecordingExecutor implements SyncSqlExecutor {
        private final List<SqlRequest> requests = new ArrayList<>();
        private final List<SqlExecutionOptions> options = new ArrayList<>();
        private final List<Long> affectedRows = new ArrayList<>();
        private int affectedRowIndex;

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            throw new UnsupportedOperationException("schema test does not query metadata");
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            requests.add(request);
            return nextAffectedRows();
        }

        @Override
        public long rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
            this.options.add(options);
            return rowsUpdated(request);
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            return new SqlWriteResult(rowsUpdated(request), List.of());
        }

        private List<String> sqlTexts() {
            return requests.stream().map(SqlRequest::sql).toList();
        }

        private List<SqlExecutionOptions> options() {
            return List.copyOf(options);
        }

        private void withAffectedRows(long... rows) {
            affectedRows.clear();
            for (long rowsUpdated : rows) {
                affectedRows.add(rowsUpdated);
            }
            affectedRowIndex = 0;
        }

        private long nextAffectedRows() {
            return affectedRowIndex < affectedRows.size() ? affectedRows.get(affectedRowIndex++) : 1L;
        }
    }

    private static final class MetadataExecutor implements SyncSqlExecutor {
        private final AtomicInteger queryCalls = new AtomicInteger();

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            queryCalls.incrementAndGet();
            if (request.sql().stripLeading().startsWith("select c.COLUMN_NAME")) {
                return List.of(DynamicRow.copyOf(Map.of(
                        "COLUMN_NAME", "id", "DATA_TYPE", "bigint",
                        "PRIMARY_KEY", true, "NULLABLE", false)));
            }
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException("metadata executor does not write");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException("metadata executor does not write");
        }
    }

    private static final class DualFailureExecutor implements SyncSqlExecutor {
        private final String workSql;
        private final RuntimeException workFailure;
        private final RuntimeException cleanupFailure;
        private boolean workFailed;

        private DualFailureExecutor(String workSql,
                                    RuntimeException workFailure,
                                    RuntimeException cleanupFailure) {
            this.workSql = workSql;
            this.workFailure = workFailure;
            this.cleanupFailure = cleanupFailure;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            throw new UnsupportedOperationException("schema test does not query metadata");
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            return rowsUpdated(request, SqlExecutionOptions.safeDefaults());
        }

        @Override
        public long rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
            if (workFailed) {
                throw cleanupFailure;
            }
            if (workSql.equals(request.sql())) {
                workFailed = true;
                throw workFailure;
            }
            return 0L;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            return new SqlWriteResult(rowsUpdated(request, options), List.of());
        }
    }

    /** 用 setup/work/cleanup 的调用时序验证 VME 不会跳过外部事务会话恢复。 */
    private static final class FatalWorkExecutor implements SyncSqlExecutor {
        private final String workSql;
        private final Throwable failure;
        private final AtomicInteger cleanupCalls = new AtomicInteger();
        private boolean workStarted;

        private FatalWorkExecutor(String workSql, Throwable failure) {
            this.workSql = workSql;
            this.failure = failure;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            throw new UnsupportedOperationException("schema test does not query metadata");
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            return rowsUpdated(request, SqlExecutionOptions.safeDefaults());
        }

        @Override
        public long rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
            if (workSql.equals(request.sql())) {
                workStarted = true;
                throw propagate(failure);
            }
            if (workStarted) {
                cleanupCalls.incrementAndGet();
            }
            return 0L;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            return new SqlWriteResult(rowsUpdated(request, options), List.of());
        }

        private int cleanupCalls() {
            return cleanupCalls.get();
        }

        private static RuntimeException propagate(Throwable failure) {
            if (failure instanceof RuntimeException runtime) {
                return runtime;
            }
            throw (Error) failure;
        }
    }
}
