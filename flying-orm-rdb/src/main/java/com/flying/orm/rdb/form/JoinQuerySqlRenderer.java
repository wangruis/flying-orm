package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.join.JoinClause;
import com.flying.orm.core.join.JoinFieldPair;
import com.flying.orm.core.join.JoinOrder;
import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.PaginationDialect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * 把不可变 JOIN AST 渲染为四库共享的参数化查询请求。
 *
 * <p>内部别名只由源序号生成。源级租户、DataScope 和逻辑删除保护先进入受控派生关系，
 * 因而 LEFT/RIGHT 链不会被最终 WHERE 意外收紧；调用方业务条件仍保留在 JOIN 之后。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class JoinQuerySqlRenderer {

    private final JoinSourceSqlRenderer sources;
    private final PaginationDialect pagination;

    JoinQuerySqlRenderer(FormSqlRenderSupport support, PaginationDialect pagination) {
        this.sources = new JoinSourceSqlRenderer(support);
        this.pagination = Objects.requireNonNull(pagination, "join pagination dialect must not be null");
    }

    SqlRequest select(JoinQuerySpec spec,
                      Map<JoinSource, com.flying.orm.core.form.DynamicForm> physicalForms,
                      Map<JoinSource, ConditionGroup> protections,
                      Map<JoinSource, ConditionGroup> businessConditions) {
        return render(spec, physicalForms, protections, businessConditions, false);
    }

    SqlRequest select(JoinQuerySpec spec, Map<JoinSource, ConditionGroup> protections) {
        JoinInputs inputs = logicalInputs(spec, protections);
        return select(spec, inputs.forms(), inputs.protections(), inputs.businessConditions());
    }

    SqlRequest select(JoinQuerySpec spec,
                      Map<JoinSource, com.flying.orm.core.form.DynamicForm> physicalForms,
                      Map<JoinSource, ConditionGroup> protections,
                      Map<JoinSource, ConditionGroup> businessConditions,
                      PageQuery page) {
        PageQuery safePage = requireJoinPage(spec, page);
        SqlRequest request = render(spec, physicalForms, protections, businessConditions, false);
        return pagination.paginate(request.sql(), request.parameters(), safePage);
    }

    SqlRequest select(JoinQuerySpec spec,
                      Map<JoinSource, ConditionGroup> protections,
                      PageQuery page) {
        JoinInputs inputs = logicalInputs(spec, protections);
        return select(spec, inputs.forms(), inputs.protections(), inputs.businessConditions(), page);
    }

    SqlRequest count(JoinQuerySpec spec,
                     Map<JoinSource, com.flying.orm.core.form.DynamicForm> physicalForms,
                     Map<JoinSource, ConditionGroup> protections,
                     Map<JoinSource, ConditionGroup> businessConditions) {
        return render(spec, physicalForms, protections, businessConditions, true);
    }

    SqlRequest count(JoinQuerySpec spec, Map<JoinSource, ConditionGroup> protections) {
        JoinInputs inputs = logicalInputs(spec, protections);
        return count(spec, inputs.forms(), inputs.protections(), inputs.businessConditions());
    }

    private SqlRequest render(JoinQuerySpec spec,
                              Map<JoinSource, com.flying.orm.core.form.DynamicForm> physicalForms,
                              Map<JoinSource, ConditionGroup> protections,
                              Map<JoinSource, ConditionGroup> businessConditions,
                              boolean count) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        Map<JoinSource, com.flying.orm.core.form.DynamicForm> safeForms = Map.copyOf(Objects.requireNonNull(
                physicalForms, "join physical forms must not be null"));
        Map<JoinSource, ConditionGroup> safeProtections = Map.copyOf(Objects.requireNonNull(
                protections, "join source protections must not be null"));
        Map<JoinSource, ConditionGroup> safeBusiness = Map.copyOf(Objects.requireNonNull(
                businessConditions, "join business conditions must not be null"));
        if (!safeSpec.sources().containsAll(safeProtections.keySet())
                || !safeSpec.sources().containsAll(safeForms.keySet())
                || !safeSpec.sources().containsAll(safeBusiness.keySet())) {
            throw new IllegalArgumentException("join protection source is not part of the query");
        }

        List<Object> parameters = new ArrayList<>();
        String projection = count ? "count(*) as total" : projections(safeSpec.projections());
        StringBuilder sql = new StringBuilder("select ").append(projection)
                                                           .append(" from ")
                                                           .append(sources.relation(
                                                                   safeSpec.root(),
                                                                   form(safeForms, safeSpec.root()),
                                                                   protection(safeProtections, safeSpec.root()),
                                                                   parameters));
        for (JoinClause join : safeSpec.joins()) {
            sql.append(' ').append(keyword(join)).append(' ')
               .append(sources.relation(join.source(),
                                        form(safeForms, join.source()),
                                        protection(safeProtections, join.source()),
                                        parameters))
               .append(" on ").append(on(join));
        }
        appendBusinessWhere(sql, safeSpec, safeForms, safeBusiness, parameters);
        if (!count) {
            appendOrder(sql, safeSpec.orders());
        }
        return new SqlRequest(sql.toString(), parameters, SqlBindMarkerStyle.CANONICAL);
    }

    private static PageQuery requireJoinPage(JoinQuerySpec spec, PageQuery page) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        PageQuery safePage = Objects.requireNonNull(page, "join page query must not be null");
        if (!safePage.sorts().isEmpty()) {
            throw new IllegalArgumentException("join page sorts must be declared with source-qualified orderBy");
        }
        if (safeSpec.orders().isEmpty()) {
            throw new IllegalArgumentException("join page requires at least one source-qualified order");
        }
        return safePage;
    }

    private static JoinInputs logicalInputs(JoinQuerySpec spec,
                                            Map<JoinSource, ConditionGroup> protections) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        Map<JoinSource, com.flying.orm.core.form.DynamicForm> forms = new java.util.LinkedHashMap<>();
        Map<JoinSource, ConditionGroup> business = new java.util.LinkedHashMap<>();
        safeSpec.sources().forEach(source -> {
            forms.put(source, source.form());
            business.put(source, safeSpec.where(source));
        });
        return new JoinInputs(forms, protections, business);
    }

    private record JoinInputs(Map<JoinSource, com.flying.orm.core.form.DynamicForm> forms,
                              Map<JoinSource, ConditionGroup> protections,
                              Map<JoinSource, ConditionGroup> businessConditions) {
    }

    private String projections(List<JoinProjection> projections) {
        StringJoiner selected = new StringJoiner(", ");
        for (JoinProjection projection : projections) {
            selected.add(sources.field(projection.field()) + " as " + sources.aliasIdentifier(projection.alias()));
        }
        return selected.toString();
    }

    private String on(JoinClause join) {
        StringJoiner conditions = new StringJoiner(" and ");
        for (JoinFieldPair pair : join.on()) {
            conditions.add(sources.field(pair.left()) + " = " + sources.field(pair.right()));
        }
        return conditions.toString();
    }

    private void appendBusinessWhere(StringBuilder sql,
                                     JoinQuerySpec spec,
                                     Map<JoinSource, com.flying.orm.core.form.DynamicForm> physicalForms,
                                     Map<JoinSource, ConditionGroup> businessConditions,
                                     List<Object> parameters) {
        StringJoiner where = new StringJoiner(" and ");
        for (JoinSource source : spec.sources()) {
            ConditionGroup condition = businessConditions.get(source);
            SqlFragment fragment = sources.businessCondition(
                    source, form(physicalForms, source), condition);
            if (!fragment.sql().isBlank()) {
                where.add(condition.operator() == LogicalOperator.OR && condition.children().size() > 1
                                  ? "(" + fragment.sql() + ")"
                                  : fragment.sql());
                parameters.addAll(fragment.parameters());
            }
        }
        if (where.length() > 0) {
            sql.append(" where ").append(where);
        }
    }

    private void appendOrder(StringBuilder sql, List<JoinOrder> orders) {
        if (orders.isEmpty()) {
            return;
        }
        StringJoiner order = new StringJoiner(", ", " order by ", "");
        for (JoinOrder item : orders) {
            order.add(sources.field(item.field()) + " " + item.direction().name().toLowerCase(java.util.Locale.ROOT));
        }
        sql.append(order);
    }

    private static ConditionGroup protection(Map<JoinSource, ConditionGroup> protections, JoinSource source) {
        return protections.getOrDefault(source, ConditionGroup.and().build());
    }

    private static com.flying.orm.core.form.DynamicForm form(
            Map<JoinSource, com.flying.orm.core.form.DynamicForm> forms,
            JoinSource source) {
        return Objects.requireNonNull(forms.get(source), "join physical form is missing");
    }

    private static String keyword(JoinClause join) {
        return switch (join.type()) {
            case INNER -> "inner join";
            case LEFT -> "left outer join";
            case RIGHT -> "right outer join";
        };
    }
}
