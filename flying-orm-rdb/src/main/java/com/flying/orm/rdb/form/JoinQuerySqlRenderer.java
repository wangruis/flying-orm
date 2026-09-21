package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.join.JoinClause;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinFieldPair;
import com.flying.orm.core.join.JoinOrder;
import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.sql.render.SqlRenderer;
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

    private static final ConditionGroup EMPTY = ConditionGroup.and().build();

    private final PaginationDialect pagination;
    private final FormSqlRenderSupport support;

    JoinQuerySqlRenderer(FormSqlRenderSupport support, PaginationDialect pagination) {
        this.support = Objects.requireNonNull(support, "form SQL render support must not be null");
        this.pagination = Objects.requireNonNull(pagination, "join pagination dialect must not be null");
    }

    SqlRequest select(JoinQuerySpec spec,
                      Map<JoinSource, com.flying.orm.core.form.DynamicForm> physicalForms,
                      Map<JoinSource, ConditionGroup> protections,
                      Map<JoinSource, ConditionGroup> businessConditions) {
        return render(spec, physicalForms, protections, businessConditions, false, true);
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
        SqlRequest request = render(spec, physicalForms, protections, businessConditions, false, false);
        SqlRequest paginated = pagination.paginate(
                request.sql(), OwnedBindableValues.ownedValues(request.parameters()), safePage);
        return support.compiledRequest(
                paginated.sql(), OwnedBindableValues.ownedValues(paginated.parameters()));
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
        return render(spec, physicalForms, protections, businessConditions, true, true);
    }

    SqlRequest count(JoinQuerySpec spec, Map<JoinSource, ConditionGroup> protections) {
        JoinInputs inputs = logicalInputs(spec, protections);
        return count(spec, inputs.forms(), inputs.protections(), inputs.businessConditions());
    }

    private SqlRequest render(JoinQuerySpec spec,
                              Map<JoinSource, com.flying.orm.core.form.DynamicForm> physicalForms,
                              Map<JoinSource, ConditionGroup> protections,
                              Map<JoinSource, ConditionGroup> businessConditions,
                              boolean count,
                              boolean compile) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        Map<JoinSource, com.flying.orm.core.form.DynamicForm> safeForms = Objects.requireNonNull(
                physicalForms, "join physical forms must not be null");
        Map<JoinSource, ConditionGroup> safeProtections = Objects.requireNonNull(
                protections, "join source protections must not be null");
        Map<JoinSource, ConditionGroup> safeBusiness = Objects.requireNonNull(
                businessConditions, "join business conditions must not be null");
        if (!safeSpec.sources().containsAll(safeProtections.keySet())
                || !safeSpec.sources().containsAll(safeForms.keySet())
                || !safeSpec.sources().containsAll(safeBusiness.keySet())) {
            throw new IllegalArgumentException("join protection source is not part of the query");
        }

        List<Object> parameters = new ArrayList<>();
        String projection = count ? "count(*) as total" : projections(safeSpec.projections());
        StringBuilder sql = new StringBuilder("select ").append(projection)
                                                           .append(" from ")
                                                           .append(relation(
                                                                   safeSpec.root(),
                                                                   form(safeForms, safeSpec.root()),
                                                                   protection(safeProtections, safeSpec.root()),
                                                                   parameters));
        for (JoinClause join : safeSpec.joins()) {
            sql.append(' ').append(keyword(join)).append(' ')
               .append(relation(join.source(),
                                        form(safeForms, join.source()),
                                        protection(safeProtections, join.source()),
                                        parameters))
               .append(" on ").append(on(join));
        }
        appendBusinessWhere(sql, safeSpec, safeForms, safeBusiness, parameters);
        if (!count) {
            appendOrder(sql, safeSpec.orders());
        }
        return compile
                ? support.compiledRequest(sql.toString(), parameters)
                : new SqlRequest(sql.toString(), parameters);
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

    private String alias(JoinSource source) {
        return support.identifier("t" + Objects.requireNonNull(
                source, "join source must not be null").ordinal());
    }

    private String field(JoinFieldRef field) {
        JoinFieldRef safeField = Objects.requireNonNull(field, "join field must not be null");
        return alias(safeField.source()) + "." + support.identifier(safeField.field());
    }

    private void requireStableOffsetTimeOrdering(JoinFieldRef field) {
        JoinFieldRef safeField = Objects.requireNonNull(field, "join field must not be null");
        support.requireStableOffsetTimeOrdering(
                safeField.source().form().field(safeField.field()));
    }

    private String aliasIdentifier(String alias) {
        // JoinProjection 已验证为单段别名；H2 的普通标识符折叠不能改变结果绑定身份。
        return "h2".equals(support.dialectName) ? '"' + alias + '"' : support.identifier(alias);
    }

    private String relation(JoinSource source,
                            com.flying.orm.core.form.DynamicForm physicalForm,
                            ConditionGroup protection,
                            List<Object> parameters) {
        JoinSource safeSource = Objects.requireNonNull(source, "join source must not be null");
        SqlFragment filter = condition(safeSource, physicalForm, protection, false);
        parameters.addAll(OwnedBindableValues.ownedValues(filter.parameters()));
        String table = support.identifier(safeSource.form());
        if (filter.sql().isBlank()) {
            return table + " " + alias(safeSource);
        }
        return "(select * from " + table + " where " + filter.sql() + ") " + alias(safeSource);
    }

    private SqlFragment businessCondition(JoinSource source,
                                          com.flying.orm.core.form.DynamicForm physicalForm,
                                          ConditionGroup condition) {
        return condition(source, physicalForm, condition, true);
    }

    /** 源内保护条件不带别名，业务条件使用来源别名；相关项沿用同一受控限定符。 */
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
            String sourceField = safeForm.field(name).name();
            return qualified
                    ? alias(safeSource) + "." + support.identifier(sourceField)
                    : support.identifier(sourceField);
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

    private String projections(List<JoinProjection> projections) {
        StringJoiner selected = new StringJoiner(", ");
        for (JoinProjection projection : projections) {
            selected.add(field(projection.field()) + " as " + aliasIdentifier(projection.alias()));
        }
        return selected.toString();
    }

    private String on(JoinClause join) {
        StringJoiner conditions = new StringJoiner(" and ");
        for (JoinFieldPair pair : join.on()) {
            conditions.add(field(pair.left()) + " = " + field(pair.right()));
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
            SqlFragment fragment = businessCondition(
                    source, form(physicalForms, source), condition);
            if (!fragment.sql().isBlank()) {
                where.add(condition.operator() == LogicalOperator.OR && condition.children().size() > 1
                                  ? "(" + fragment.sql() + ")"
                                  : fragment.sql());
                parameters.addAll(OwnedBindableValues.ownedValues(fragment.parameters()));
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
            requireStableOffsetTimeOrdering(item.field());
            order.add(field(item.field()) + " " + item.direction().name().toLowerCase(java.util.Locale.ROOT));
        }
        sql.append(order);
    }

    private static ConditionGroup protection(Map<JoinSource, ConditionGroup> protections, JoinSource source) {
        return protections.getOrDefault(source, EMPTY);
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
