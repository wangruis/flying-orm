package com.flying.orm.core.condition;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.util.Objects;

/**
 * 结构化条件编译失败时抛出的稳定异常。
 *
 * <p>除了给人看的 message，它还保留错误码、前端属性路径、字段和 operator。调用方可以直接把
 * {@link #toErrorReport()} 转成统一接口错误，不需要解析异常文本；例如值类型错误可以精确指向
 * {@code conditions[2].value}。</p>
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public final class StructuredConditionException extends IllegalArgumentException implements OrmErrorReportProvider {

    private final StructuredConditionErrorCode code;

    private final String path;

    private final String field;

    private final String operator;

    private StructuredConditionException(StructuredConditionErrorCode code,
                                         String path,
                                         String field,
                                         String operator,
                                         String message,
                                         Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code, "structured condition error code must not be null");
        this.path = path;
        this.field = field;
        this.operator = operator;
    }

    /** 创建只包含错误码和路径的通用条件异常。 */
    public static StructuredConditionException of(StructuredConditionErrorCode code,
                                                  String path,
                                                  String message) {
        return new StructuredConditionException(code, path, null, null, message, null);
    }

    /** 创建与具体字段有关的条件异常。 */
    public static StructuredConditionException field(StructuredConditionErrorCode code,
                                                     String path,
                                                     String field,
                                                     String message) {
        return new StructuredConditionException(code, path, field, null, message, null);
    }

    /** 创建与具体操作符有关的条件异常。 */
    public static StructuredConditionException operator(StructuredConditionErrorCode code,
                                                        String path,
                                                        String operator,
                                                        String message) {
        return new StructuredConditionException(code, path, null, operator, message, null);
    }

    /** 创建同时保留字段和操作符的条件异常。 */
    public static StructuredConditionException term(StructuredConditionErrorCode code,
                                                    String path,
                                                    String field,
                                                    String operator,
                                                    String message) {
        return new StructuredConditionException(code, path, field, operator, message, null);
    }

    /** 创建保留底层类型转换或 codec 异常的条件异常。 */
    public static StructuredConditionException cause(StructuredConditionErrorCode code,
                                                     String path,
                                                     String field,
                                                     String operator,
                                                     String message,
                                                     Throwable cause) {
        return new StructuredConditionException(code, path, field, operator, message, cause);
    }

    public StructuredConditionErrorCode code() {
        return code;
    }

    /** @return 前端输入中的属性路径，无法定位时可能为 null */
    public String path() {
        return path;
    }

    public String field() {
        return field;
    }

    public String operator() {
        return operator;
    }

    /** @return 跨模块统一使用的 CONDITION 类错误报告 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("CONDITION", code.name(), null, path, field, getMessage());
    }
}
