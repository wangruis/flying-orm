package com.flying.orm.rdb.exception;

import com.flying.orm.core.condition.StructuredConditionErrorCode;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrors;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.rdb.batch.BatchOptimisticLockException;
import com.flying.orm.rdb.batch.BatchRowConflict;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.execution.SqlLargeObjectLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.lock.OptimisticLockConflictException;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.schema.SchemaMigrationFailureCode;
import com.flying.orm.rdb.schema.SchemaMigrationRejectedException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 确保条件、scope 和数据库异常对上层暴露同一套稳定错误报告形状。 */
class ErrorReportContractTest {

    @Test
    void exposesOneStableReportShapeForConditionScopeAndDatabaseErrors() {
        OrmErrorReport condition = StructuredConditionException.field(
                StructuredConditionErrorCode.FIELD_NOT_ALLOWED,
                "conditions[0].field",
                "tenant_id",
                "field is protected").toErrorReport();
        OrmErrorReport scope = new ScopeAccessException(
                ScopeErrorCode.TENANT_SCOPE_REQUIRED,
                "userForm",
                "tenant_id",
                "tenant scope is required").toErrorReport();
        OrmErrorReport database = new RdbException(
                RdbErrorKind.DUPLICATE_KEY,
                "duplicate key",
                "23505",
                1062,
                new IllegalStateException("driver error")).toErrorReport();

        assertEquals("CONDITION", condition.category());
        assertEquals("FIELD_NOT_ALLOWED", condition.code());
        assertEquals("conditions[0].field", condition.path());
        assertEquals("SCOPE", scope.category());
        assertEquals("userForm", scope.resource());
        assertEquals("DATABASE", database.category());
        assertEquals("DUPLICATE_KEY", database.code());
        assertEquals("23505", database.resource());
    }

    @Test
    void findsStableReportsAcrossExecutionBatchMappingAndMigrationErrors() {
        OrmErrorReport timeout = OrmErrors.report(new SqlExecutionTimeoutException(
                Duration.ofSeconds(2), new IllegalStateException("timeout"))).orElseThrow();
        OrmErrorReport rowLimit = OrmErrors.report(new SqlRowLimitExceededException(
                SqlStatementType.SELECT, 100, 100)).orElseThrow();
        OrmErrorReport lobLimit = OrmErrors.report(new SqlLargeObjectLimitExceededException(
                SqlLargeObjectLimitExceededException.Kind.BINARY, 1024, 2048)).orElseThrow();
        OrmErrorReport sequence = OrmErrors.report(new SqlExecutionSequenceException(
                SqlExecutionPhase.CLEANUP, 1, List.of(), new IllegalStateException("cleanup"))).orElseThrow();
        OrmErrorReport optimistic = OrmErrors.report(new OptimisticLockConflictException(
                "users", "version", 3L)).orElseThrow();
        OrmErrorReport batchOptimistic = OrmErrors.report(new BatchOptimisticLockException(
                List.of(BatchRowConflict.exactlyOne(7, 0)))).orElseThrow();
        OrmErrorReport batch = OrmErrors.report(new BatchWriteException(
                "batch outcome is unknown",
                new IllegalStateException("connection lost"),
                new BatchWriteResult(BatchWriteOptions.Mode.ATOMIC,
                                     BatchWriteResult.Status.UNKNOWN,
                                     8,
                                     0,
                                     List.of()))).orElseThrow();
        OrmErrorReport mapping = OrmErrors.report(new MappingException("missing constructor")).orElseThrow();
        OrmErrorReport migration = OrmErrors.report(new SchemaMigrationRejectedException(
                SchemaMigrationFailureCode.APPROVAL_REQUIRED,
                "fingerprint-1",
                "exact approval is required")).orElseThrow();

        assertEquals("EXECUTION", timeout.category());
        assertEquals("TIMEOUT", timeout.code());
        assertEquals("PT2S", timeout.resource());
        assertEquals("ROW_LIMIT_EXCEEDED", rowLimit.code());
        assertEquals("rows[100]", rowLimit.path());
        assertEquals("LARGE_OBJECT_LIMIT_EXCEEDED", lobLimit.code());
        assertEquals("BINARY", lobLimit.resource());
        assertEquals("SEQUENCE_CLEANUP_FAILED", sequence.code());
        assertEquals("steps[1]", sequence.path());
        assertEquals("OPTIMISTIC_LOCK", optimistic.category());
        assertEquals("users", optimistic.resource());
        assertEquals("version", optimistic.field());
        assertEquals("BATCH_OPTIMISTIC_LOCK", batchOptimistic.code());
        assertEquals("rows[7]", batchOptimistic.path());
        assertEquals("BATCH", batch.category());
        assertEquals("WRITE_UNKNOWN", batch.code());
        assertEquals("ATOMIC", batch.resource());
        assertEquals("MAPPING", mapping.category());
        assertEquals("MAPPING_FAILED", mapping.code());
        assertEquals("MIGRATION", migration.category());
        assertEquals("APPROVAL_REQUIRED", migration.code());
        assertEquals("fingerprint-1", migration.resource());
    }

    /** 验证冲突异常不在 message 中回显无界标识符，但保留结构化表和字段定位。 */
    @Test
    void keepsOptimisticLockLocationOutsideTheStableErrorMessage() {
        String table = "t".repeat(5_000);
        String field = "f".repeat(5_000);
        OptimisticLockConflictException error = new OptimisticLockConflictException(table, field, 3L);
        OrmErrorReport report = OrmErrors.report(error).orElseThrow();

        assertEquals("optimistic lock conflict", error.getMessage());
        assertEquals(table, error.table());
        assertEquals(field, error.field());
        assertEquals(table, report.resource());
        assertEquals(field, report.field());
        assertEquals("optimistic lock conflict", report.message());
    }

    /** 批量异常可源自调用方 Publisher，协议错误报告不能复制无界调用方消息。 */
    @Test
    void keepsCallerBatchMessageOutOfThePublicErrorReport() {
        String secret = "caller-secret-".repeat(500);
        BatchWriteException error = new BatchWriteException(
                secret,
                new IllegalStateException("source failed"),
                new BatchWriteResult(BatchWriteOptions.Mode.ATOMIC,
                                     BatchWriteResult.Status.UNKNOWN,
                                     0,
                                     0,
                                     List.of()));

        OrmErrorReport report = error.toErrorReport();

        assertEquals(secret, error.getMessage());
        assertEquals("batch write failed", report.message());
        assertFalse(report.message().contains(secret));
    }

    /** 映射异常可能包含实体类型或枚举配置，协议错误报告不能复制无界调用方诊断。 */
    @Test
    void keepsCallerMappingMessageOutOfThePublicErrorReport() {
        String secret = "mapping-secret-".repeat(500);
        MappingException error = new MappingException(secret);

        OrmErrorReport report = error.toErrorReport();

        assertEquals(secret, error.getMessage());
        assertEquals("mapping failed", report.message());
        assertFalse(report.message().contains(secret));
    }

    /** 驱动诊断保留在异常对象中，协议报告只允许固定消息和合法的五位 SQLState。 */
    @Test
    void keepsUnboundedDatabaseDiagnosticsOutOfThePublicErrorReport() {
        String secret = "driver-secret-".repeat(500);
        RdbException error = new RdbException(
                RdbErrorKind.CONNECTION,
                secret,
                secret,
                0,
                new IllegalStateException("driver failure"));

        OrmErrorReport report = error.toErrorReport();

        assertEquals(secret, error.getMessage());
        assertEquals(secret, error.sqlState());
        assertEquals("database operation failed", report.message());
        assertNull(report.resource());
        assertFalse(report.message().contains(secret));
    }

    @Test
    void unwrapsKnownOrmErrorWithoutLoopingForeverOnCauseCycles() {
        RuntimeException wrapped = new RuntimeException("service wrapper",
                new OptimisticLockConflictException("users", "version", 3L));

        OrmErrorReport report = OrmErrors.report(wrapped).orElseThrow();

        assertEquals("OPTIMISTIC_LOCK", report.category());
        assertEquals("CONFLICT", report.code());

        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");
        first.initCause(second);
        second.initCause(first);
        assertTrue(OrmErrors.report(first).isEmpty());
    }
}
