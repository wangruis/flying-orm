package com.flying.orm.rdb.exception;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTimeoutException;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * 把数据库驱动异常翻成 flying-orm 稳定的错误分类，让调用方不必依赖某个驱动的异常子类。
 *
 * <p>分类优先使用标准 SQLState，再补 MySQL、PostgreSQL、Oracle 和 SQL Server 的常见厂商错误码。
 * 原始异常始终保留为 cause，便于日志排查。无法确认的错误归为 UNKNOWN，不根据错误文本做脆弱猜测。</p>
 *
 * @author wangr
 * @date 2026-07-26
 * @version v1.0
 */
public final class RdbExceptionTranslator {

    private RdbExceptionTranslator() {
    }

    /**
     * 翻译数据库异常。已有 RdbException 原样返回，普通业务 RuntimeException 也不会被错误包装。
     *
     * @param error 执行链抛出的异常
     * @return 可直接向上抛出的稳定运行时异常
     */
    public static RuntimeException translate(Throwable error) {
        Throwable safeError = Objects.requireNonNull(error, "rdb error must not be null");
        Throwable databaseError = databaseCause(safeError);
        if (databaseError instanceof RdbException translated) {
            return translated;
        }
        if (databaseError instanceof R2dbcException r2dbcError) {
            RdbErrorKind kind = kind(r2dbcError);
            return new RdbException(kind,
                                    message(kind),
                                    r2dbcError.getSqlState(),
                                    r2dbcError.getErrorCode(),
                                    r2dbcError);
        }
        if (databaseError instanceof SQLTimeoutException sqlTimeout) {
            return new RdbException(RdbErrorKind.TIMEOUT,
                                    message(RdbErrorKind.TIMEOUT),
                                    sqlTimeout.getSQLState(),
                                    sqlTimeout.getErrorCode(),
                                    sqlTimeout);
        }
        if (databaseError instanceof SQLException sqlError) {
            RdbErrorKind kind = kind(sqlError);
            return new RdbException(kind,
                                    message(kind),
                                    sqlError.getSQLState(),
                                    sqlError.getErrorCode(),
                                    sqlError);
        }
        if (databaseError instanceof TimeoutException) {
            return new RdbException(RdbErrorKind.TIMEOUT,
                                    "database operation timed out",
                                    null,
                                    null,
                                    databaseError);
        }
        if (safeError instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new RdbException(RdbErrorKind.UNKNOWN,
                                "database operation failed",
                                null,
                                null,
                                safeError);
    }

    /**
     * 只拆 JDK 明确定义为异步容器的异常。业务异常本身可能携带批量结果，不能因为它有 cause 就把外层丢掉。
     */
    private static Throwable databaseCause(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException || current instanceof ExecutionException) {
            Throwable cause = current.getCause();
            if (cause == null || cause == current) {
                break;
            }
            current = cause;
        }
        return current;
    }

    private static RdbErrorKind kind(R2dbcException error) {
        // SPI 的明确异常类型比可能同时表示“用户取消”与“数据库超时”的通用 SQLState 更精确。
        if (error instanceof R2dbcTimeoutException) {
            return RdbErrorKind.TIMEOUT;
        }
        RdbErrorKind classified = kind(error.getSqlState(), error.getErrorCode());
        if (classified != RdbErrorKind.UNKNOWN) {
            return classified;
        }
        // 有些驱动确认连接已经坏掉，却不给 SQLState。标准资源异常比驱动私有类名稳定，适合作为最后兜底。
        return error instanceof R2dbcNonTransientResourceException
                ? RdbErrorKind.CONNECTION
                : RdbErrorKind.UNKNOWN;
    }

    private static RdbErrorKind kind(SQLException error) {
        RdbErrorKind classified = kind(error.getSQLState(), error.getErrorCode());
        if (classified != RdbErrorKind.UNKNOWN) {
            return classified;
        }
        // SQLState 缺失时，JDBC 标准连接异常类型可以提供比 UNKNOWN 更可靠的兜底信息。
        return hasText(error.getSQLState()) || !isConnectionException(error)
                ? RdbErrorKind.UNKNOWN
                : RdbErrorKind.CONNECTION;
    }

    private static boolean isConnectionException(SQLException error) {
        return error instanceof SQLNonTransientConnectionException
                || error instanceof SQLTransientConnectionException
                || error instanceof SQLRecoverableException;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static RdbErrorKind kind(String sqlState, int errorCode) {
        // 顺序有意义：例如 SQL Server 1205 在事务回滚类 SQLState 下是死锁，其他情况更像锁等待超时。
        if ("23505".equals(sqlState)
                || errorCode == 1
                || errorCode == 1062
                || errorCode == 2601
                || errorCode == 2627) {
            return RdbErrorKind.DUPLICATE_KEY;
        }
        if ("40P01".equals(sqlState)
                || errorCode == 60
                || errorCode == 1213
                || (errorCode == 1205 && sqlState != null && sqlState.startsWith("40"))) {
            return RdbErrorKind.DEADLOCK;
        }
        if ("55P03".equals(sqlState)
                || errorCode == 1205
                || errorCode == 1222
                // MySQL 8 执行 SELECT ... FOR UPDATE NOWAIT 时返回 ER_LOCK_NOWAIT。
                || errorCode == 3572
                || errorCode == 30006) {
            return RdbErrorKind.LOCK_TIMEOUT;
        }
        if ("57014".equals(sqlState) || "HY008".equals(sqlState) || errorCode == 1013) {
            return RdbErrorKind.CANCELLED;
        }
        if ((sqlState != null && sqlState.startsWith("23"))
                || errorCode == 547
                || errorCode == 515
                || errorCode == 1400
                || errorCode == 1451
                || errorCode == 1452
                || errorCode == 2291
                || errorCode == 2292
                || errorCode == 2290) {
            return RdbErrorKind.CONSTRAINT;
        }
        if (sqlState != null && sqlState.startsWith("42")) {
            return RdbErrorKind.BAD_SQL;
        }
        if (sqlState != null && (sqlState.startsWith("08") || sqlState.startsWith("57P0"))) {
            return RdbErrorKind.CONNECTION;
        }
        if ("HYT00".equals(sqlState) || "HYT01".equals(sqlState)) {
            return RdbErrorKind.TIMEOUT;
        }
        return RdbErrorKind.UNKNOWN;
    }

    private static String message(RdbErrorKind kind) {
        // 对外消息保持稳定且不带 SQL/参数，详细驱动消息仍可从 cause 获取。
        return switch (kind) {
            case DUPLICATE_KEY -> "database duplicate key conflict";
            case CONSTRAINT -> "database integrity constraint failed";
            case BAD_SQL -> "database rejected sql";
            case CONNECTION -> "database connection failed";
            case TIMEOUT -> "database operation timed out";
            case DEADLOCK -> "database transaction deadlocked";
            case LOCK_TIMEOUT -> "database lock wait timed out";
            case CANCELLED -> "database operation was cancelled";
            case UNKNOWN -> "database operation failed";
        };
    }
}
