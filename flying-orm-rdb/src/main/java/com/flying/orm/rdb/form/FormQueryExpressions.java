package com.flying.orm.rdb.form;

import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.sql.expression.SqlExpressionCompiler;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.internal.value.ExpressionFieldValue;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Set;
import java.util.function.Function;

/** 查询投影的 SQL 与结果字段事实；最终解码由读取规划器统一准备。 */
final class FormQueryExpressions {
    private FormQueryExpressions() { }

    static Projection prepare(FormDataSqlRenderer renderer, QuerySpec spec, ScopedRead read,
                               SensitiveDisplayMode displayMode) {
        return prepare(renderer, spec, read, displayMode, renderer.conditionRenderer()::identifier);
    }

    private static Projection prepare(FormDataSqlRenderer renderer, QuerySpec spec, ScopedRead read,
                                        SensitiveDisplayMode displayMode, Function<String, String> identifier) {
        DynamicForm form = spec.form();
        DynamicForm.Builder result = DynamicForm.relationalBuilder(form.id(), form.relationIdentity())
                .namingSource(form.namingSource()).features(form.features());
        form.fields().forEach(result::addField);
        form.protections().encryptedFields().forEach(result::encrypted);
        form.protections().maskedFields().forEach(result::masked);
        form.tenant().ifPresent(tenant -> result.tenant(tenant.fieldName(), tenant.strategy()));
        form.logicDelete().ifPresent(logic -> result.logicDelete(
                logic.fieldName(), logic.notDeletedValue(), logic.deletedValue()));
        StringJoiner columns = new StringJoiner(", ");
        List<Object> parameters = new ArrayList<>();
        List<String> selected = FormQueryShapeGuard.projectionFields(spec, read.form());
        List<String> output = new ArrayList<>(selected.size());
        for (String name : selected) {
            ValueExpression expression = spec.expressions().get(FieldIdentity.of(name).key());
            if (expression == null) {
                String field = read.form().field(name).name();
                columns.add(identifier.apply(field));
                output.add(field);
                continue;
            }
            SqlExpressionCompiler.Operand compiled = compile(renderer, spec, read, displayMode, identifier, expression);
            columns.add(compiled.fragment().sql() + " as " + renderer.conditionRenderer().identifier(name));
            parameters.addAll(compiled.fragment().parameters());
            DynamicField resultField = expression instanceof ValueExpression.Field reference
                    ? DynamicField.of(name, form.field(reference.identity().name()).databaseType())
                    : FormDataSqlRenderer.expressionField(name, compiled.type());
            result.addField(resultField);
            output.add(name);
        }
        Map<String, SqlFragment> ordering = new HashMap<>();
        Set<String> outputKeys = new HashSet<>();
        output.forEach(field -> outputKeys.add(FieldIdentity.of(field).key()));
        for (PageSort sort : spec.sorts()) {
            String key = FieldIdentity.of(sort.field()).key();
            ValueExpression expression = spec.expressions().get(key);
            if (expression != null && !outputKeys.contains(key) && !ordering.containsKey(key)) {
                SqlExpressionCompiler.Operand compiled = compile(renderer, spec, read, displayMode, identifier, expression);
                ordering.put(key, compiled.fragment());
                result.addField(FormDataSqlRenderer.expressionField(sort.field(), compiled.type()));
            }
        }
        List<String> outputFields = List.copyOf(output);
        return new Projection(new SqlFragment(columns.toString(), parameters), result.build(), outputFields,
                outputFields, Map.copyOf(ordering));
    }

    private static SqlExpressionCompiler.Operand compile(FormDataSqlRenderer renderer, QuerySpec spec,
            ScopedRead read, SensitiveDisplayMode displayMode, Function<String, String> identifier,
            ValueExpression expression) {
        return renderer.compileExpression(expression, reference -> {
            if (reference.source() != null) {
                throw new IllegalArgumentException("query expression requires fields from the current form");
            }
            DynamicField field = read.form().field(reference.identity().name());
            requireComputedSource(spec.form(), field, displayMode);
            if (!spec.groups().isEmpty() && spec.groups().stream().noneMatch(group -> group.equalsIgnoreCase(field.name()))) {
                throw new IllegalArgumentException("computed grouped query requires grouped source fields");
            }
            return new SqlExpressionCompiler.Operand(new SqlFragment(identifier.apply(field.name()), List.of()),
                    field.databaseType().isArray() ? LogicalType.OTHER : field.databaseType().logicalType());
        });
    }

    /** 内部复核列不改变公开投影，最终解码字段包含计算别名与复核列。 */
    static Projection prepareContains(FormDataSqlRenderer renderer, QuerySpec spec, ScopedRead read,
                                         SensitiveDisplayMode displayMode,
                                         ProtectedFieldRuntime.PreparedContainsQuery contains) {
        ProtectedContainsSqlPlanner planner = renderer.protection().contains;
        Projection projection = prepare(renderer, spec, read, displayMode, planner::projectionField);
        StringJoiner columns = new StringJoiner(", ").add(projection.columns().sql());
        List<String> decodedFields = new ArrayList<>(projection.outputFields());
        Set<String> selected = new HashSet<>();
        projection.outputFields().forEach(field -> selected.add(FieldIdentity.of(field).key()));
        for (String field : contains.visibleFields()) {
            if (selected.add(FieldIdentity.of(field).key())) {
                columns.add(planner.projectionField(field) + " as " + renderer.conditionRenderer().identifier(field));
                decodedFields.add(field);
            }
        }
        return new Projection(new SqlFragment(columns.toString(), projection.columns().parameters()),
                projection.resultForm(), projection.outputFields(),
                List.copyOf(decodedFields), projection.ordering());
    }

    static Projection prepareJoin(FormDataSqlRenderer renderer, List<JoinProjection> projections,
                                    Map<JoinSource, DynamicForm> physicalForms, SensitiveDisplayMode displayMode) {
        DynamicForm.Builder result = DynamicForm.builder("join-result", "join_result");
        StringJoiner columns = new StringJoiner(", ");
        List<Object> parameters = new ArrayList<>();
        JoinQuerySqlRenderer joins = renderer.joinQueries();
        for (JoinProjection projection : projections) {
            String sql;
            if (projection.expression() == null) {
                DynamicField source = physicalForms.get(projection.field().source()).field(projection.field().field());
                ColumnDefinition column = source.column();
                result.addField(new DynamicField(ColumnDefinition.builder(projection.alias(), source.databaseType())
                        .length(column.length()).precision(column.precision()).scale(column.scale())
                        .temporalPrecision(column.temporalPrecision()).build(), false, false));
                sql = joins.field(projection.field());
            } else {
                SqlExpressionCompiler.Operand compiled = renderer.compileExpression(ExpressionFieldValue.bindPriorities(
                        projection.expression(), reference -> reference.source().form().field(reference.identity().name())), reference -> {
                    DynamicForm source = reference.source().form();
                    DynamicField field = source.field(reference.identity().name());
                    requireComputedSource(source, field, displayMode);
                    return new SqlExpressionCompiler.Operand(new SqlFragment(
                            joins.field(new JoinFieldRef(reference.source(), field.name())), List.of()),
                            field.databaseType().isArray() ? LogicalType.OTHER : field.databaseType().logicalType());
                });
                sql = compiled.fragment().sql();
                parameters.addAll(compiled.fragment().parameters());
                result.addField(FormDataSqlRenderer.expressionField(projection.alias(), compiled.type()));
            }
            columns.add(sql + " as " + joins.aliasIdentifier(projection.alias()));
        }
        return new Projection(new SqlFragment(columns.toString(), parameters), result.build(),
                projections.stream().map(JoinProjection::alias).toList());
    }

    private static void requireComputedSource(DynamicForm form, DynamicField field, SensitiveDisplayMode displayMode) {
        FormUpdateExpressions.requirePlain(form, field);
        form.protections().masked(field.name()).ifPresent(mask -> {
            if (displayMode == SensitiveDisplayMode.MASKED || displayMode == SensitiveDisplayMode.DECLARED
                    && mask.display() == SensitiveDisplayMode.MASKED) {
                throw new IllegalArgumentException("computed projection requires full source visibility");
            }
        });
    }

    record Projection(SqlFragment columns, DynamicForm resultForm, List<String> outputFields,
                      List<String> decodingFields, Map<String, SqlFragment> ordering) {
        Projection(SqlFragment columns, DynamicForm resultForm, List<String> outputFields) {
            this(columns, resultForm, outputFields, outputFields, Map.of());
        }

        SqlFragment orderBy(List<PageSort> sorts, Function<String, String> identifier) {
            if (sorts.isEmpty()) return new SqlFragment("", List.of());
            StringJoiner sql = new StringJoiner(", ", " order by ", "");
            List<Object> parameters = new ArrayList<>();
            for (PageSort sort : sorts) {
                SqlFragment expression = ordering.get(FieldIdentity.of(sort.field()).key());
                sql.add((expression == null ? identifier.apply(sort.field()) : expression.sql()) + " " + sort.sqlKeyword());
                if (expression != null) parameters.addAll(expression.parameters());
            }
            return new SqlFragment(sql.toString(), parameters);
        }
    }
}
