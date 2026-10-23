package com.flying.orm.rdb.array;

import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermExtensionDescriptor;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderContext;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.sql.render.SqlTermPackage;

import java.util.Set;

/**
 * PostgreSQL 一维数组条件包。SQL 模板固定，数组元素始终作为一个驱动参数绑定，
 * 调用方不能从条件值里拼接操作符或字段名。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class ArrayTermHandlers {

    private ArrayTermHandlers() {
    }

    /**
     * 创建 PostgreSQL 数组条件包：包含、被包含、相交及其否定形式，以及 any-equals。
     * 否定使用 SQL NOT，保留 NULL 的三值逻辑；空数组仍作为有类型的参数绑定。
     *
     * @return 可注册到 {@code SqlRenderer} 的条件包
     */
    public static SqlTermPackage postgresql() {
        return SqlTermPackage.of("postgresql-array",
                                 array(ArrayStructuredConditions.CONTAINS, "@>", false),
                                 array(ArrayStructuredConditions.CONTAINED_BY, "<@", false),
                                 array(ArrayStructuredConditions.OVERLAPS, "&&", false),
                                 array(ArrayStructuredConditions.NOT_CONTAINS, "@>", true),
                                 array(ArrayStructuredConditions.NOT_CONTAINED_BY, "<@", true),
                                 array(ArrayStructuredConditions.NOT_OVERLAPS, "&&", true),
                                 SqlTermHandler.of(descriptor(ArrayStructuredConditions.ANY_EQUALS),
                                                   ConditionValueShape.SCALAR,
                                                   ArrayTermHandlers::renderAnyEquals));
    }

    private static SqlTermHandler array(String id, String operator, boolean negated) {
        return SqlTermHandler.of(descriptor(id), ConditionValueShape.SCALAR, (term, context) -> {
            ArrayConditionValue value = value(term);
            return SqlFragment.of((negated ? "not (" : "") + context.identifier(term.field()) + " " + operator
                                        + " cast(? as " + value.postgresqlCastType() + ")" + (negated ? ")" : ""),
                                  value.parameter());
        });
    }

    private static TermExtensionDescriptor descriptor(String id) {
        return TermExtensionDescriptor.filter(id, Set.of(), 1, 1);
    }

    private static SqlFragment renderAnyEquals(TermCondition term, SqlRenderContext context) {
        return SqlFragment.of("? = any(" + context.identifier(term.field()) + ")", term.value());
    }

    private static ArrayConditionValue value(TermCondition term) {
        if (term.value() instanceof ArrayConditionValue value) {
            return value;
        }
        throw new IllegalArgumentException("array term value must be ArrayConditionValue");
    }
}
