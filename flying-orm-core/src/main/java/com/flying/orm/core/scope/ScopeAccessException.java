package com.flying.orm.core.scope;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.io.Serial;
import java.util.Objects;

/**
 * 数据范围拒绝操作时抛这个异常。它属于调用参数不满足安全范围，并同时提供稳定的结构化错误信息。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class ScopeAccessException extends IllegalArgumentException implements OrmErrorReportProvider {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ScopeErrorCode code;

    private final String formId;

    private final String field;

    /**
     * 创建一条可以准确定位到表单和字段的范围错误。
     *
     * @param code 稳定错误码
     * @param formId 动态表单标识
     * @param field 相关字段，没有就传 null
     * @param message 给开发人员看的说明
     */
    public ScopeAccessException(ScopeErrorCode code, String formId, String field, String message) {
        super(Objects.requireNonNull(message, "scope error message must not be null"));
        this.code = Objects.requireNonNull(code, "scope error code must not be null");
        this.formId = Objects.requireNonNull(formId, "scope form id must not be null");
        this.field = field;
    }

    /** @return 稳定错误码 */
    public ScopeErrorCode code() {
        return code;
    }

    /** @return 出错的动态表单标识 */
    public String formId() {
        return formId;
    }

    /** @return 相关字段，没有时为 null */
    public String field() {
        return field;
    }

    /** @return 统一的 SCOPE 类错误报告 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("SCOPE", code.name(), formId, null, field, getMessage());
    }
}
