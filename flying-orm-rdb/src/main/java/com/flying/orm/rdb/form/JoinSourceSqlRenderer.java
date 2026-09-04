package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;

import java.util.Objects;

/**
 * 为单个 JOIN 数据源渲染稳定内部别名、限定字段和源内保护条件。
 *
 * <p>源内条件使用普通字段名渲染到受控派生关系；业务条件使用限定字段渲染到最终 WHERE。
 * 两条路径共享同一个 term 注册表和 codec，不允许带点字段字符串绕过标识符校验。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class JoinSourceSqlRenderer {

    private final FormSqlRenderSupport support;

    JoinSourceSqlRenderer(FormSqlRenderSupport support) {
        this.support = Objects.requireNonNull(support, "form SQL render support must not be null");
    }

    String alias(JoinSource source) {
        return support.identifier("t" + Objects.requireNonNull(source, "join source must not be null").ordinal());
    }

    String field(JoinFieldRef field) {
        JoinFieldRef safeField = Objects.requireNonNull(field, "join field must not be null");
        return alias(safeField.source()) + "." + support.identifier(safeField.field());
    }

    void requireStableOffsetTimeOrdering(JoinFieldRef field) {
        JoinFieldRef safeField = Objects.requireNonNull(field, "join field must not be null");
        support.requireStableOffsetTimeOrdering(safeField.source().form().field(safeField.field()));
    }

    String aliasIdentifier(String alias) {
        return support.identifier(alias);
    }

    String relation(JoinSource source,
                    com.flying.orm.core.form.DynamicForm physicalForm,
                    ConditionGroup protection,
                    java.util.List<Object> parameters) {
        JoinSource safeSource = Objects.requireNonNull(source, "join source must not be null");
        SqlFragment filter = condition(safeSource, physicalForm, protection, false);
        parameters.addAll(filter.parameters());
        String table = support.identifier(safeSource.form());
        if (filter.sql().isBlank()) {
            return table + " " + alias(safeSource);
        }
        return "(select * from " + table + " where " + filter.sql() + ") " + alias(safeSource);
    }

    SqlFragment businessCondition(JoinSource source,
                                  com.flying.orm.core.form.DynamicForm physicalForm,
                                  ConditionGroup condition) {
        return condition(source, physicalForm, condition, true);
    }

    private SqlFragment condition(JoinSource source,
                                  com.flying.orm.core.form.DynamicForm physicalForm,
                                  ConditionGroup condition,
                                  boolean qualified) {
        JoinSource safeSource = Objects.requireNonNull(source, "join source must not be null");
        com.flying.orm.core.form.DynamicForm safeForm = Objects.requireNonNull(
                physicalForm, "join physical form must not be null");
        ConditionGroup safeCondition = support.normalizeCondition(
                safeForm, Objects.requireNonNull(condition, "join condition must not be null"));
        SqlRenderer renderer = support.normalizedConditionRenderer().withFieldIdentifierRenderer(name -> {
            String field = safeForm.field(name).name();
            return qualified
                    ? alias(safeSource) + "." + support.identifier(field)
                    : support.identifier(field);
        });
        if (!renderer.hasCorrelatedTerms()) {
            return renderer.renderWhere(safeCondition);
        }
        String qualifier = qualified ? alias(safeSource) : support.identifier(safeForm);
        return renderer.renderWhere(
                safeCondition,
                name -> qualifier + "." + support.identifier(safeForm.field(name).name()),
                name -> qualifier);
    }
}
