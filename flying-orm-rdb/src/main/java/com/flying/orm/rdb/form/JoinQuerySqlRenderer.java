package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.expression.SqlExpressionCompiler;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.internal.value.ExpressionFieldValue;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.join.JoinClause;
import com.flying.orm.core.join.JoinCondition;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinOrder;
import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.dialect.DialectCapabilityId;
import com.flying.orm.rdb.lock.LockingReadDialect;
import com.flying.orm.rdb.lock.ReadLock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.ArrayDeque;

/**
 * 把不可变 JOIN AST 渲染为四库共享的参数化查询请求。
 *
 * <p>内部别名只由源序号生成。源级租户、DataScope 和逻辑删除保护先进入受控派生关系，
 * 因而外连接不会被最终 WHERE 意外收紧；调用方业务条件仍保留在 JOIN 之后。</p>
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

    SqlRequest select(JoinQuerySpec spec, Map<JoinSource, ConditionGroup> protections) {
        Map<JoinSource, com.flying.orm.core.form.DynamicForm> forms = new java.util.LinkedHashMap<>();
        spec.sources().forEach(source -> forms.put(source, source.form()));
        return select(spec, relation(spec, forms, protections, Map.of()));
    }

    SqlRequest select(JoinQuerySpec spec, SqlFragment relation) {
        return select(spec, relation, new SqlFragment(projections(spec.projections()), List.of()));
    }

    SqlRequest select(JoinQuerySpec spec, SqlFragment relation, SqlFragment columns) {
        return select(spec, relation, columns, null, null, null, null);
    }

    SqlRequest select(JoinQuerySpec spec, SqlFragment relation, SqlFragment columns, PageQuery page,
                      LockingReadDialect locking, ReadLock lock, DynamicForm physicalRoot) {
        List<Object> parameters = parameters(spec, columns, relation);
        String sql = selectSql(spec, relation, columns.sql(), parameters);
        if (page != null) {
            SqlRequest paginated = pagination.paginate(sql, parameters, page);
            sql = paginated.sql();
            parameters = OwnedBindableValues.ownedValues(paginated.parameters());
        }
        if (lock != null) sql += lockingSuffix(spec.root(), physicalRoot, locking, lock);
        return support.compiledRequest(sql, parameters);
    }

    SqlRequest select(JoinQuerySpec spec, SqlFragment relation, PageQuery page, SqlFragment columns) {
        return select(spec, relation, columns, requireJoinPage(spec, page), null, null, null);
    }

    SqlRequest selectKeyset(JoinQuerySpec spec, JoinQueryPlanner.PreparedSources sources, SqlFragment columns,
            KeysetPageNormalizer.NormalizedKeysetPage page, Map<String, JoinFieldRef> fields,
            LockingReadDialect locking, ReadLock lock) {
        if (lock != null && !locking.supportsPagination(lock)) {
            throw new UnsupportedOperationException("locking keyset pagination is not supported by this database descriptor");
        }
        for (KeysetSort sort : page.sorts()) {
            JoinFieldRef field = fields.get(sort.field());
            support.requireStableOffsetTimeOrdering(field.source().form().field(field.field()));
        }
        SqlFragment cursor = FormKeysetSqlRenderer.cursorPredicate(page, name -> field(fields.get(name)), (name, value) -> {
            JoinFieldRef field = fields.get(name);
            return support.writeValue(field.source().form().field(field.field()), value);
        });
        SqlFragment relation = relation(spec, sources.physicalForms(), sources.protections(), sources.valueConditions(),
                locking, lock, cursor);
        List<Object> parameters = parameters(spec, columns, relation);
        String sql = "select " + columns.sql() + " " + relation.sql()
                + FormKeysetSqlRenderer.orderBy(page, name -> field(fields.get(name)));
        SqlRequest paginated = pagination.paginate(sql, parameters, PageQuery.of(1, Math.addExact(page.size(), 1)));
        return support.compiledRequest(paginated.sql() + (lock == null ? ""
                : lockingSuffix(spec.root(), sources.physicalForms().get(spec.root()), locking, lock)),
                OwnedBindableValues.ownedValues(paginated.parameters()));
    }

    SqlRequest count(SqlFragment relation) {
        String count = "sqlserver".equals(support.dialectName) ? "count_big" : "count";
        return support.compiledRequest("select " + count + "(*) as total " + relation.sql(),
                OwnedBindableValues.ownedValues(relation.parameters()));
    }

    SqlRequest count(JoinQuerySpec spec, SqlFragment relation) {
        if (spec.groups().isEmpty()) return count(relation);
        StringBuilder grouped = new StringBuilder("from (select 1 as grouped_value ").append(relation.sql());
        appendGroups(grouped, spec.groups());
        return count(new SqlFragment(grouped.append(") grouped_query").toString(), relation.parameters()));
    }

    private static List<Object> parameters(JoinQuerySpec spec, SqlFragment columns, SqlFragment relation) {
        List<Object> relationParameters = OwnedBindableValues.ownedValues(relation.parameters());
        if (columns.parameters().isEmpty() && spec.orders().stream().noneMatch(order -> order.expression() != null)) {
            return relationParameters;
        }
        List<Object> parameters = new ArrayList<>(columns.parameters());
        parameters.addAll(relationParameters);
        return parameters;
    }

    private String selectSql(JoinQuerySpec spec, SqlFragment relation, String columns, List<Object> parameters) {
        StringBuilder sql = new StringBuilder("select ").append(columns)
                .append(' ').append(relation.sql());
        appendGroups(sql, spec.groups());
        appendOrder(sql, spec.orders(), parameters);
        return sql.toString();
    }

    private void appendGroups(StringBuilder sql, List<JoinFieldRef> groups) {
        if (groups.isEmpty()) return;
        StringJoiner grouped = new StringJoiner(", ", " group by ", "");
        for (JoinFieldRef group : groups) grouped.add(field(group));
        sql.append(grouped);
    }

    /** 来源保护、ON 和业务 WHERE 的共同关系输入；不包含投影、排序或分页。 */
    SqlFragment relation(JoinQuerySpec spec,
                         Map<JoinSource, com.flying.orm.core.form.DynamicForm> physicalForms,
                         Map<JoinSource, ConditionGroup> protections,
                         Map<JoinCondition.Values, ConditionGroup> valueConditions) {
        return relation(spec, physicalForms, protections, valueConditions, null, null);
    }

    SqlFragment relation(JoinQuerySpec spec, Map<JoinSource, DynamicForm> physicalForms,
                         Map<JoinSource, ConditionGroup> protections,
                         Map<JoinCondition.Values, ConditionGroup> valueConditions,
                         LockingReadDialect locking, ReadLock lock) {
        return relation(spec, physicalForms, protections, valueConditions, locking, lock, null);
    }

    private SqlFragment relation(JoinQuerySpec spec, Map<JoinSource, DynamicForm> physicalForms,
                         Map<JoinSource, ConditionGroup> protections,
                         Map<JoinCondition.Values, ConditionGroup> valueConditions,
                         LockingReadDialect locking, ReadLock lock, SqlFragment cursor) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        Map<JoinSource, com.flying.orm.core.form.DynamicForm> safeForms = Objects.requireNonNull(
                physicalForms, "join physical forms must not be null");
        Map<JoinSource, ConditionGroup> safeProtections = Objects.requireNonNull(
                protections, "join source protections must not be null");
        if (!safeSpec.sources().containsAll(safeProtections.keySet())
                || !safeSpec.sources().containsAll(safeForms.keySet())) {
            throw new IllegalArgumentException("join protection source is not part of the query");
        }

        List<Object> parameters = new ArrayList<>();
        StringBuilder sql = new StringBuilder("from ").append(lock != null
                ? support.identifier(safeSpec.root().form()) + " " + alias(safeSpec.root()) + locking.tableHint(lock)
                : relation(
                safeSpec.root(), form(safeForms, safeSpec.root()),
                protection(safeProtections, safeSpec.root()), parameters));
        for (JoinClause join : safeSpec.joins()) {
            if (lock != null && (join.type() == JoinType.RIGHT || join.type() == JoinType.FULL)) {
                throw new UnsupportedOperationException("locking read requires a non-nullable root source");
            }
            sql.append(' ').append(keyword(join)).append(' ')
               .append(relation(join.source(),
                                        form(safeForms, join.source()),
                                        protection(safeProtections, join.source()),
                                        parameters))
               .append(" on ").append(condition(join.condition(), safeForms, valueConditions, parameters));
        }
        SqlFragment rootFilter = lock == null ? null : condition(safeSpec.root(), form(safeForms, safeSpec.root()),
                protection(safeProtections, safeSpec.root()), true);
        boolean filteredRoot = rootFilter != null && !rootFilter.sql().isBlank();
        if (filteredRoot) {
            sql.append(" where (").append(rootFilter.sql()).append(')');
            parameters.addAll(OwnedBindableValues.ownedValues(rootFilter.parameters()));
        }
        if (safeSpec.where() != null) {
            String where = condition(safeSpec.where(), safeForms, valueConditions, parameters);
            sql.append(filteredRoot ? " and (" + where + ")"
                    : cursor != null ? " where (" + where + ")" : " where " + where);
        }
        if (cursor != null && !cursor.sql().isBlank()) {
            sql.append(filteredRoot || safeSpec.where() != null ? " and (" : " where (")
                    .append(cursor.sql()).append(')');
            parameters.addAll(cursor.parameters());
        }
        return new SqlFragment(sql.toString(), parameters);
    }

    /** 普通查询的锁只落在根来源；列目标方言同样锁整行，不锁计算投影。 */
    private String lockingSuffix(JoinSource root, DynamicForm physicalRoot, LockingReadDialect dialect, ReadLock lock) {
        String target = alias(root);
        if ("oracle".equals(support.dialectName)) target += "." + support.identifier(physicalRoot.fields().getFirst().name());
        return dialect.suffix(lock, target);
    }

    static PageQuery requireJoinPage(JoinQuerySpec spec, PageQuery page) {
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

    private String alias(JoinSource source) {
        return support.identifier("t" + Objects.requireNonNull(
                source, "join source must not be null").ordinal());
    }

    String field(JoinFieldRef field) {
        JoinFieldRef safeField = Objects.requireNonNull(field, "join field must not be null");
        return alias(safeField.source()) + "." + support.identifier(safeField.field());
    }

    private void requireStableOffsetTimeOrdering(JoinFieldRef field) {
        JoinFieldRef safeField = Objects.requireNonNull(field, "join field must not be null");
        support.requireStableOffsetTimeOrdering(
                safeField.source().form().field(safeField.field()));
    }

    String aliasIdentifier(String alias) {
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
        }, safeForm::field);
        if (!renderer.hasCorrelatedTerms()) {
            return renderer.renderWhere(safeCondition);
        }
        String qualifier = qualified ? alias(safeSource) : support.identifier(safeForm);
        return renderer.renderWhere(
                safeCondition,
                name -> qualifier + "." + support.identifier(FormSqlRenderSupport.correlatedField(safeForm, name)),
                name -> qualifier);
    }

    private String projections(List<JoinProjection> projections) {
        StringJoiner selected = new StringJoiner(", ");
        for (JoinProjection projection : projections) {
            selected.add(field(projection.field()) + " as " + aliasIdentifier(projection.alias()));
        }
        return selected.toString();
    }

    private String condition(JoinCondition condition,
                               Map<JoinSource, com.flying.orm.core.form.DynamicForm> physicalForms,
                               Map<JoinCondition.Values, ConditionGroup> prepared,
                               List<Object> parameters) {
        StringBuilder sql = new StringBuilder();
        ArrayDeque<ConditionFrame> frames = new ArrayDeque<>();
        frames.addLast(new ConditionFrame(List.of(condition), "", false));
        while (!frames.isEmpty()) {
            ConditionFrame frame = frames.getLast();
            if (frame.index == frame.children.size()) {
                if (frame.grouped) sql.append(')');
                frames.removeLast();
                continue;
            }
            if (frame.index > 0) sql.append(frame.separator);
            JoinCondition node = frame.children.get(frame.index++);
            if (node instanceof JoinCondition.Group group) {
                sql.append('(');
                frames.addLast(new ConditionFrame(group.children(),
                        group.operator() == LogicalOperator.AND ? " and " : " or ", true));
            } else if (node instanceof JoinCondition.Comparison comparison) {
                if (!comparison.operator().equals("=") && !comparison.operator().equals("<>")) {
                    requireStableOffsetTimeOrdering(comparison.left());
                    requireStableOffsetTimeOrdering(comparison.right());
                }
                sql.append(field(comparison.left())).append(' ').append(comparison.operator())
                        .append(' ').append(field(comparison.right()));
            } else if (node instanceof JoinCondition.Values values) {
                SqlFragment fragment = businessCondition(values.source(), form(physicalForms, values.source()),
                        prepared.getOrDefault(values, values.condition()));
                sql.append('(').append(fragment.sql().isBlank() ? "1 = 1" : fragment.sql()).append(')');
                parameters.addAll(OwnedBindableValues.ownedValues(fragment.parameters()));
            }
        }
        return sql.toString();
    }

    private static final class ConditionFrame {
        private final List<JoinCondition> children;
        private final String separator;
        private final boolean grouped;
        private int index;

        private ConditionFrame(List<JoinCondition> children, String separator, boolean grouped) {
            this.children = children;
            this.separator = separator;
            this.grouped = grouped;
        }
    }

    private void appendOrder(StringBuilder sql, List<JoinOrder> orders, List<Object> parameters) {
        if (orders.isEmpty()) {
            return;
        }
        StringJoiner order = new StringJoiner(", ", " order by ", "");
        for (JoinOrder item : orders) {
            String ordered;
            if (item.expression() == null) {
                requireStableOffsetTimeOrdering(item.field());
                ordered = field(item.field());
            } else {
                SqlExpressionCompiler.Operand compiled = support.expression(ExpressionFieldValue.bindPriorities(
                        item.expression(), reference -> reference.source().form().field(reference.identity().name())), reference -> {
                    DynamicField source = reference.source().form().field(reference.identity().name());
                    FormUpdateExpressions.requirePlain(reference.source().form(), source);
                    return new SqlExpressionCompiler.Operand(new SqlFragment(field(
                            new JoinFieldRef(reference.source(), source.name())), List.of()),
                            source.databaseType().isArray() ? LogicalType.OTHER : source.databaseType().logicalType());
                });
                if (compiled.type() == LogicalType.OFFSET_TIME) {
                    support.requireStableOffsetTimeOrdering(FormDataSqlRenderer.expressionField("ordering", compiled.type()));
                }
                ordered = compiled.fragment().sql();
                parameters.addAll(compiled.fragment().parameters());
            }
            order.add(ordered + " " + item.direction().name().toLowerCase(java.util.Locale.ROOT));
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

    private String keyword(JoinClause join) {
        return switch (join.type()) {
            case INNER -> "inner join";
            case LEFT -> "left outer join";
            case RIGHT -> "right outer join";
            case FULL -> {
                if (!support.dialectCapabilities.supports(DialectCapabilityId.FULL_OUTER_JOIN)) {
                    throw new IllegalArgumentException("dialect does not declare FULL OUTER JOIN support: "
                            + support.dialectName);
                }
                yield "full outer join";
            }
        };
    }
}
