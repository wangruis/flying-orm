package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicForm;

import java.util.Objects;

/**
 * 给前端结构化条件编译加一点扩展：可以改输入，也可以改安全策略。
 * 比如 JSON 条件要先把 Map 变成值对象，业务 term 只需要放行 operator。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public interface StructuredConditionCustomizer {

    /**
     * 在可信表单元数据下适配前端条件。只修改 operator 白名单的定制器可以沿用这个默认实现；
     * 需要转换值的定制器必须覆盖本方法，并根据表单字段验证输入。
     */
    default StructuredConditionInput adapt(DynamicForm form, StructuredConditionInput input) {
        Objects.requireNonNull(form, "dynamic form must not be null");
        return Objects.requireNonNull(input, "structured condition input must not be null");
    }

    default StructuredConditionPolicy customize(StructuredConditionPolicy policy) {
        return Objects.requireNonNull(policy, "structured condition policy must not be null");
    }

    static StructuredConditionCustomizer allowOperator(String operator) {
        return allowOperator(operator, operator);
    }

    static StructuredConditionCustomizer allowOperator(String externalOperator, String internalOperator) {
        Objects.requireNonNull(externalOperator, "external condition operator must not be null");
        Objects.requireNonNull(internalOperator, "internal condition operator must not be null");
        return new StructuredConditionCustomizer() {

            @Override
            public StructuredConditionPolicy customize(StructuredConditionPolicy policy) {
                return StructuredConditionCustomizer.super.customize(policy)
                                                   .allowOperator(externalOperator, internalOperator);
            }
        };
    }
}
