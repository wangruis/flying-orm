package com.flying.orm.rdb.form;



import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.sql.expression.SqlExpressionCompiler;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.protection.SensitiveDisplayMode;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** 已解析的聚合操作数：只负责来源治理和叶子适配，表达式 SQL 由共享编译器生成。 */
record AggregateInput(SqlFragment fragment, List<AggregateField> sources, DynamicField valueField) {

    FieldVisibility visibility(FieldUseSnapshot snapshot) {
        FieldVisibility result = FieldVisibility.FULL;
        if (!snapshot.isUnrestricted()) {
            for (AggregateField source : sources) {
                FieldVisibility current = source.visibility(snapshot);
                if (current == FieldVisibility.HIDDEN) return current;
                if (current == FieldVisibility.MASKED) result = current;
            }
        }
        return result;
    }

    void requireVisibility(FieldUseSnapshot fieldUse, SensitiveDisplayMode displayMode,
                           boolean changedType, boolean computed) {
        if (!fieldUse.isUnrestricted() && changedType && sources.stream()
                .anyMatch(source -> source.visibility(fieldUse) == FieldVisibility.MASKED)) {
            throw new IllegalArgumentException(
                    "computed or COUNT output cannot inherit MASKED visibility from its source field");
        }
        if (computed && displayMode != SensitiveDisplayMode.FULL) {
            for (AggregateField source : sources) {
                if (source.visibility(fieldUse) != FieldVisibility.HIDDEN && source.form().protections()
                        .masked(source.field().name()).filter(mask -> displayMode == SensitiveDisplayMode.MASKED
                                || mask.display() == SensitiveDisplayMode.MASKED).isPresent()) {
                    throw new IllegalArgumentException("computed aggregate requires full source visibility");
                }
            }
        }
    }

    static AggregateInput field(AggregateField source) {
        return new AggregateInput(new SqlFragment(source == null ? "" : source.expression(), List.of()),
                source == null ? List.of() : List.of(source), source == null ? null : source.field());
    }

    static AggregateInput computed(ValueExpression expression,
                                   Function<ValueExpression.Field, AggregateField> resolver,
                                   FormAggregateReadSupport reads) {
        if (expression instanceof ValueExpression.Field field) {
            return field(resolver.apply(field));
        }
        Set<AggregateField> sources = new LinkedHashSet<>();
        SqlExpressionCompiler.Operand compiled = reads.renderer.compileExpression(expression, field -> {
            AggregateField source = resolver.apply(field);
            sources.add(source);
            LogicalType type = source.field().databaseType().isArray() ? LogicalType.OTHER
                    : source.field().databaseType().logicalType();
            return new SqlExpressionCompiler.Operand(new SqlFragment(source.expression(), List.of()), type);
        });
        LogicalType type = compiled.type() == null ? LogicalType.DECIMAL : compiled.type();
        return new AggregateInput(compiled.fragment(), List.copyOf(sources), FormDataSqlRenderer.expressionField("aggregate_operand", type));
    }

}
