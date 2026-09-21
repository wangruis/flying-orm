package com.flying.orm.rdb.exception;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.util.Objects;

/**
 * flying-orm 对外抛出的关系型数据库异常，保留 SQLState 和错误码方便排查。
 *
 * @author wangr
 * @date 2026-07-26
 * @version v1.0
 */
public class RdbException extends RuntimeException implements OrmErrorReportProvider {

    private static final long serialVersionUID = 1L;

    private final RdbErrorKind kind;

    private final String sqlState;

    private final Integer errorCode;

    /**
     * 包装驱动异常，同时保留上层排障需要的原始错误信息。
     *
     * @param kind 稳定错误分类
     * @param message 简短说明
     * @param sqlState 驱动 SQLState，没有时为 null
     * @param errorCode 数据库原始数字错误码，没有时为 null
     * @param cause 原始异常
     */
    public RdbException(RdbErrorKind kind, String message, String sqlState, Integer errorCode, Throwable cause) {
        super(Objects.requireNonNull(message, "rdb error message must not be null"),
              Objects.requireNonNull(cause, "rdb error cause must not be null"));
        this.kind = Objects.requireNonNull(kind, "rdb error kind must not be null");
        this.sqlState = sqlState;
        this.errorCode = errorCode;
    }

    /** @return 上层可以稳定判断的数据库错误分类 */
    public RdbErrorKind kind() {
        return kind;
    }

    /** @return 驱动 SQLState，没有时为 null */
    public String sqlState() {
        return sqlState;
    }

    /** @return 数据库原始数字错误码，没有时为 null */
    public Integer errorCode() {
        return errorCode;
    }

    /** @return 统一的 DATABASE 类错误报告 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("DATABASE",
                                  kind.name(),
                                  reportSqlState(sqlState),
                                  null,
                                  null,
                                  "database operation failed");
    }

    private static String reportSqlState(String value) {
        if (value == null || value.length() != 5) {
            return null;
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!(character >= '0' && character <= '9')
                    && !(character >= 'A' && character <= 'Z')) {
                return null;
            }
        }
        return value;
    }
}
