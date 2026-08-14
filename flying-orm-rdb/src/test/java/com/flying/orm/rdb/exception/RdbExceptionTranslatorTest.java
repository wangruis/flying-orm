package com.flying.orm.rdb.exception;

import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * 各数据库会用不同的 SQLState 和错误码描述同一件事，这里守住 flying-orm 对外给出的稳定分类。
 */
class RdbExceptionTranslatorTest {

    @Test
    void classifiesDeadlocksAcrossSupportedDatabases() {
        assertKind("DEADLOCK", sqlError("40P01", 0));
        assertKind("DEADLOCK", sqlError("40001", 1213));
        assertKind("DEADLOCK", sqlError("61000", 60));
        assertKind("DEADLOCK", sqlError("40001", 1205));
    }

    @Test
    void classifiesLockWaitTimeoutsAcrossSupportedDatabases() {
        assertKind("LOCK_TIMEOUT", sqlError("55P03", 0));
        assertKind("LOCK_TIMEOUT", sqlError("HY000", 1205));
        // MySQL 8 的 NOWAIT 不会走普通锁等待超时码，而是返回 ER_LOCK_NOWAIT(3572)。
        assertKind("LOCK_TIMEOUT", sqlError("HY000", 3572));
        assertKind("LOCK_TIMEOUT", sqlError("72000", 30006));
        assertKind("LOCK_TIMEOUT", sqlError("HYT00", 1222));
    }

    @Test
    void classifiesCancellationWithoutCallingItATimeout() {
        assertKind("CANCELLED", sqlError("57014", 0));
        assertKind("CANCELLED", sqlError("72000", 1013));
        assertKind("CANCELLED", sqlError("HY008", 0));
    }

    @Test
    void classifiesDriverConnectionFailuresWithoutRelyingOnlyOnSqlStateClass08() {
        assertKind("CONNECTION", sqlError("08006", 0));
        // PostgreSQL 管理连接终止会话时返回 admin_shutdown，而不是 08 开头的连接 SQLState。
        assertKind("CONNECTION", sqlError("57P01", 0));

        // MySQL 连接被服务端关闭时驱动不给 SQLState，只能依靠 R2DBC 的标准资源异常类型判断。
        RdbException resourceError = translated(new R2dbcNonTransientResourceException("connection closed"));
        assertEquals("CONNECTION", resourceError.kind().name());
    }

    @Test
    void classifiesIntegrityConstraintsAfterDuplicateKeys() {
        assertKind("DUPLICATE_KEY", sqlError("23000", 1));
        assertKind("DUPLICATE_KEY", sqlError("23000", 2601));
        assertKind("DUPLICATE_KEY", sqlError("23000", 2627));
        assertKind("CONSTRAINT", sqlError("23503", 0));
        assertKind("CONSTRAINT", sqlError("23000", 2290));
        assertKind("CONSTRAINT", sqlError("23000", 1400));
        assertKind("CONSTRAINT", sqlError("23000", 515));
        assertKind("CONSTRAINT", sqlError("23000", 1452));
        assertKind("CONSTRAINT", sqlError("23000", 2291));
        assertKind("CONSTRAINT", sqlError("23000", 547));
    }

    @Test
    void classifiesJdbcExceptionsWithTheSameSqlStateAndVendorCodeRules() {
        assertKind("DEADLOCK", jdbcError("40P01", 0));
        assertKind("LOCK_TIMEOUT", jdbcError("HY000", 1205));
        assertKind("CANCELLED", jdbcError("57014", 0));
        assertKind("DUPLICATE_KEY", jdbcError("23000", 1062));
        assertKind("CONSTRAINT", jdbcError("23503", 0));
        assertKind("BAD_SQL", jdbcError("42000", 0));
        assertKind("CONNECTION", jdbcError("08006", 0));
    }

    @Test
    void usesJdbcTimeoutAndConnectionTypesOnlyWhenClassificationFieldsAreMissing() {
        SQLException timeout = new SQLTimeoutException("query timed out");
        RdbException translatedTimeout = translated(timeout);
        assertEquals(RdbErrorKind.TIMEOUT, translatedTimeout.kind());
        assertSame(timeout, translatedTimeout.getCause());

        SQLException connection = new SQLNonTransientConnectionException("connection closed");
        assertEquals(RdbErrorKind.CONNECTION, translated(connection).kind());
        SQLException transientConnection = new SQLTransientConnectionException("connection unavailable", "", 0);
        assertEquals(RdbErrorKind.CONNECTION, translated(transientConnection).kind());

        SQLException unknown = new SQLException("driver did not classify the failure");
        assertEquals(RdbErrorKind.UNKNOWN, translated(unknown).kind());
    }

    @Test
    void unwrapsDriverErrorsAndFeedsTheSameObservationCategory() {
        R2dbcException sqlError = sqlError("40P01", 0);
        RdbException translated = translated(new CompletionException("async wrapper", sqlError));

        assertSame(sqlError, translated.getCause());
        assertEquals("DEADLOCK", translated.kind().name());
        assertEquals("DEADLOCK", SqlFailureCategory.classify(translated).name());
    }

    @Test
    void classifiesDatabaseErrorsWrappedByBatchOrAsyncLayers() {
        RdbException deadlock = new RdbException(RdbErrorKind.DEADLOCK,
                                                 "database deadlock",
                                                 "40P01",
                                                 0,
                                                 new DriverException("deadlock", null, 0));

        assertEquals(SqlFailureCategory.DEADLOCK,
                     SqlFailureCategory.classify(new IllegalStateException("batch failed", deadlock)));
        assertEquals(SqlFailureCategory.TIMEOUT,
                     SqlFailureCategory.classify(new IllegalStateException(
                             "batch timed out",
                             new SqlExecutionTimeoutException(Duration.ofSeconds(1), new TimeoutException()))));
    }

    private static void assertKind(String expected, R2dbcException error) {
        assertEquals(expected, translated(error).kind().name());
    }

    private static void assertKind(String expected, SQLException error) {
        assertEquals(expected, translated(error).kind().name());
    }

    private static RdbException translated(Throwable error) {
        return assertInstanceOf(RdbException.class, RdbExceptionTranslator.translate(error));
    }

    private static R2dbcException sqlError(String sqlState, int errorCode) {
        return new DriverException("driver error", sqlState, errorCode);
    }

    private static SQLException jdbcError(String sqlState, int errorCode) {
        return new SQLException("driver error", sqlState, errorCode);
    }

    /** 测试只需要标准 R2DBC 错误字段，不绑定任何具体数据库驱动。 */
    private static final class DriverException extends R2dbcException {
        private DriverException(String message, String sqlState, int errorCode) {
            super(message, sqlState, errorCode);
        }
    }

}
