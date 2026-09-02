package com.flying.orm.rdb.lock;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.util.Objects;

/**
 * 版本条件没匹配上时抛这个。
 *
 * <p>它表示“这行已经被别人改过或删过”，不是 SQL 语法错，也不是数据库连接错。</p>
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
public final class OptimisticLockConflictException extends RuntimeException implements OrmErrorReportProvider {

    private final String table;

    private final String field;

    private final Object expectedValue;

    public OptimisticLockConflictException(String table, String field, Object expectedValue) {
        super("optimistic lock conflict");
        this.table = requireText(table, "optimistic lock table");
        this.field = requireText(field, "optimistic lock field");
        this.expectedValue = Objects.requireNonNull(expectedValue, "optimistic lock expected value must not be null");
    }

    public String table() {
        return table;
    }

    public String field() {
        return field;
    }

    public Object expectedValue() {
        return expectedValue;
    }

    /** @return 只暴露表和版本字段，不把期望版本值写入通用报告 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("OPTIMISTIC_LOCK", "CONFLICT", table, null, field, getMessage());
    }

    private static String requireText(String value, String name) {
        String safeValue = Objects.requireNonNull(value, name + " must not be null").trim();
        if (safeValue.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return safeValue;
    }
}
