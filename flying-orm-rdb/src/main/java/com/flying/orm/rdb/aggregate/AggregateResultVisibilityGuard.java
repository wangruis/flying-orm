package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.scope.FieldVisibility;

import java.util.List;

/**
 * 校验聚合结果与源字段发布策略是否兼容。
 *
 * <p>COUNT 的公开结果类型固定为 {@link Long}，不能交给源文本字段的 masker。该约束在 SQL 执行前
 * 统一拒绝；MIN/MAX 仍按源字段类型解码和脱敏。</p>
 */
final class AggregateResultVisibilityGuard {

    private AggregateResultVisibilityGuard() {
    }

    static void validate(List<AggregateExpression<?>> aggregates,
                         List<DynamicField> fields,
                         FieldUseSnapshot fieldUse) {
        for (int index = 0; index < aggregates.size(); index++) {
            AggregateFunction function = aggregates.get(index).function();
            if ((function == AggregateFunction.COUNT || function == AggregateFunction.COUNT_DISTINCT)
                    && fieldUse.visibility(fields.get(index).name()) == FieldVisibility.MASKED) {
                throw new IllegalArgumentException(
                        "COUNT output cannot inherit MASKED visibility from its source field");
            }
        }
    }
}
