package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.sql.expression.SqlExpressionCompiler;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.internal.dialect.DatabaseProduct;
import com.flying.orm.rdb.internal.plan.SqlStatementCompiler;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import com.flying.orm.rdb.internal.value.ExpressionFieldValue;
import com.flying.orm.rdb.lock.OptimisticLockMode;
import com.flying.orm.rdb.lock.OptimisticLockOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * 动态表单单条写入的内部渲染器。
 *
 * <p>它只负责把 insert、update 和 delete 组织成参数化 SQL；字段识别、值转换、条件编译和计划缓存
 * 统一交给 {@link FormSqlRenderSupport}。对象在门面创建时生成，之后不保存请求级状态，
 * 因而可以被多个线程并发复用。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class FormWriteSqlRenderer {

    private static final String EXPRESSION_SHAPE = ":expression:";

    private final FormSqlRenderSupport support;

    FormWriteSqlRenderer(FormSqlRenderSupport support) {
        this.support = Objects.requireNonNull(support, "form SQL render support must not be null");
    }

    /**
     * 渲染单条 insert，字段顺序沿用通用写入字段校验的结果。
     */
    SqlRequest insert(DynamicForm form, Map<String, Object> values) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        FormSqlRenderSupport.WriteFields row = support.writeFields(safeForm, values, null);
        List<DynamicField> layout = row.fields();
        Object[] parameters = row.parameters();
        for (int index = 0; index < layout.size(); index++) {
            for (int parameter = row.parameterIndex(index); parameter < row.parameterIndex(index + 1); parameter++) {
                parameters[parameter] = typedValue(layout.get(index), parameters[parameter]);
            }
        }
        return new SqlRequest(insertStatement(support, safeForm, row),
                OwnedBindableValues.publishOwned(Arrays.asList(parameters)));
    }

    /** 单条和批量 INSERT 使用同一列布局、占位符及结构缓存。 */
    static SqlStatementPlan insertStatement(FormSqlRenderSupport support, DynamicForm form,
                                            FormSqlRenderSupport.WriteFields row) {
        List<DynamicField> layout = row.fields();
        List<String> fields = support.sqlPlansEnabled ? support.fieldNames(layout) : List.of();
        boolean expressions = !row.expressions().isEmpty();
        if (expressions && support.sqlPlansEnabled) {
            fields = new ArrayList<>(fields);
            fields.addAll(row.expressions());
        }
        return support.statement(expressions ? "insert-expression" : "insert", form, fields, FormSqlRenderSupport.ConditionSql.none(),
                "", "", "", row.parameters().length, () -> {
                    if (layout.isEmpty()) {
                        String suffix = switch (DatabaseProduct.detect(support.dialectName)) {
                            case MYSQL -> " () values ()";
                            case ORACLE -> {
                                if (form.fields().isEmpty()) throw new IllegalArgumentException(
                                        "Oracle default insert requires a declared column");
                                yield " (" + support.identifier(form.fields().getFirst().name()) + ") values (default)";
                            }
                            default -> " default values";
                        };
                        // DEFAULT VALUES 不是可重复 VALUES 元组，保留驱动逐条执行语义。
                        return SqlStatementCompiler.compile("insert into " + support.identifier(form) + suffix,
                                0, SqlBindMarkerStyle.CANONICAL, support.dialectName);
                    }
                    StringJoiner columns = new StringJoiner(", ");
                    StringJoiner placeholders = new StringJoiner(", ");
                    for (int index = 0; index < layout.size(); index++) {
                        columns.add(support.identifier(layout.get(index).name()));
                        placeholders.add(row.expression(index, support));
                    }
                    return SqlStatementCompiler.insert("insert into " + support.identifier(form)
                            + " (" + columns + ") values ", "(" + placeholders + ")",
                            row.parameters().length, support.dialectName);
                });
    }

    /**
     * 渲染带业务 where 的更新。
     */
    SqlRequest update(DynamicForm form, Map<String, Object> values, ConditionGroup where) {
        return updateRequest(form, values, where, null);
    }

    /**
     * 渲染乐观锁更新。版本字段的 INCREMENT 和 ASSIGN 语义保持在 SQL 和参数顺序中。
     */
    SqlRequest update(DynamicForm form,
                      Map<String, Object> values,
                      ConditionGroup where,
                      OptimisticLockOptions lock) {
        return updateRequest(form, values, where,
                Objects.requireNonNull(lock, "optimistic lock options must not be null"));
    }

    /** 普通与乐观锁更新共用赋值规划；锁只追加版本 SET 和条件。 */
    private SqlRequest updateRequest(DynamicForm form, Map<String, Object> values,
                                     ConditionGroup where, OptimisticLockOptions lock) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        DynamicField lockField = lock == null ? null : safeForm.field(lock.field());
        if (lockField != null) requireUnencryptedLockField(safeForm, lockField);
        Map<String, Object> safeValues = Objects.requireNonNull(values, "dynamic form values must not be null");
        List<Object> parameters = new ArrayList<>(safeValues.size() + (lock == null ? 0 : 2));
        List<String> fields = new ArrayList<>(2 * (safeValues.size() + (lock == null ? 0 : 1)));
        updateAssignments(safeForm, safeValues, fields, parameters);
        int assignmentEnd = fields.size();
        if (lockField != null) requireLockFieldNotAssigned(fields, lockField);
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        List<Object> whereParameters = OwnedBindableValues.ownedValues(whereFragment.parameters());
        if (lock != null) {
            fields.add(lockField.name());
            fields.add(lock.mode() == OptimisticLockMode.INCREMENT ? ":lock-increment" : ":lock-assign");
            if (lock.mode() == OptimisticLockMode.ASSIGN) {
                parameters.add(typedValue(lockField, support.writeValue(lockField, lock.nextValue())));
            }
        }
        parameters.addAll(whereParameters);
        if (lock != null) parameters.add(typedValue(lockField, support.writeValue(lockField, lock.expectedValue())));
        return support.request(lock == null ? "update" : "update-optimistic", safeForm, fields, whereFragment, "", "", "",
                               OwnedBindableValues.publishOwned(parameters), () -> {
                                   StringJoiner sets = new StringJoiner(", ");
                                   for (int index = 0; index < assignmentEnd; index += 2) {
                                       sets.add(updateExpression(safeForm, fields.get(index), fields.get(index + 1)));
                                   }
                                   String predicate = groupedForAdditionalPredicate(where, whereFragment.sql());
                                   if (lock != null) {
                                       String column = support.identifier(lockField.name());
                                       sets.add(column + " = "
                                               + (lock.mode() == OptimisticLockMode.INCREMENT ? column + " + 1" : "?"));
                                       predicate += (predicate.isBlank() ? "" : " and ") + column + " = ?";
                                   }
                                   return "update " + support.identifier(safeForm) + " set " + sets
                                           + (predicate.isBlank() ? "" : " where " + predicate);
                               });
    }

    /**
     * 根据已经渲染好的更新请求固定后续批量行的参数类型和绑定布局。
     */
    BatchUpdatePlan batchUpdatePlan(DynamicForm form,
                                         Map<String, Object> values,
                                         OptimisticLockOptions lock,
                                         SqlRequest request) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        DynamicField lockField = lock == null ? null : safeForm.field(lock.field());
        SqlRequest safeRequest = Objects.requireNonNull(request, "batch update request must not be null");
        List<Object> parameters = OwnedBindableValues.ownedValues(safeRequest.parameters());
        List<Class<?>> parameterTypes = new ArrayList<>(parameters.size());
        if (values.values().stream().anyMatch(ValueExpression.class::isInstance)) {
            for (Object parameter : parameters) {
                parameterTypes.add(FormSqlRenderSupport.boundParameterType(parameter));
            }
            return new BatchUpdatePlan(safeRequest.statement(), parameterTypes,
                    lock == null ? BatchRowCountPolicy.ANY : BatchRowCountPolicy.EXACTLY_ONE);
        }
        // 首行已完成字段、锁和 where 校验；只恢复类型布局，不再次编码已准备好的值。
        for (String fieldName : values.keySet()) {
            parameterTypes.add(support.parameterType(safeForm.field(fieldName)));
        }
        if (lock != null && lock.mode() == OptimisticLockMode.ASSIGN) {
            parameterTypes.add(support.parameterType(lockField));
        }
        // SET（含可选 next version）之后是已编码的 WHERE；锁开启时末尾另有 expected version。
        int whereEnd = parameters.size() - (lock == null ? 0 : 1);
        for (int index = parameterTypes.size(); index < whereEnd; index++) {
            Object value = parameters.get(index);
            parameterTypes.add(FormSqlRenderSupport.boundParameterType(value));
        }
        if (lockField != null) {
            parameterTypes.add(support.parameterType(lockField));
        }
        return new BatchUpdatePlan(safeRequest.statement(), parameterTypes,
                lock == null ? BatchRowCountPolicy.ANY : BatchRowCountPolicy.EXACTLY_ONE);
    }

    /**
     * 渲染物理删除；空条件表示调用方选择全部行。
     */
    SqlRequest delete(DynamicForm form, ConditionGroup where) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        return support.request("delete", safeForm, List.of(), whereFragment, "", "", "",
                               whereFragment.parameters(), () -> "delete from "
                                       + support.identifier(safeForm)
                                       + (whereFragment.sql().isBlank() ? "" : " where " + whereFragment.sql()));
    }

    /**
     * 渲染带乐观锁版本条件的删除，版本字段只参与 where，不会被修改。
     */
    SqlRequest delete(DynamicForm form, ConditionGroup where, OptimisticLockOptions lock) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        DynamicField lockField = safeForm.field(safeLock.field());
        requireUnencryptedLockField(safeForm, lockField);
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        List<Object> whereParameters = OwnedBindableValues.ownedValues(whereFragment.parameters());
        List<Object> parameters = new ArrayList<>(whereParameters.size() + 1);
        parameters.addAll(whereParameters);
        parameters.add(typedValue(lockField, support.writeValue(lockField, safeLock.expectedValue())));
        return support.request("delete-optimistic", safeForm, List.of(lockField.name()), whereFragment, "", "", "",
                               parameters, () -> {
                                   String predicate = groupedForAdditionalPredicate(where, whereFragment.sql());
                                   return "delete from " + support.identifier(safeForm) + " where "
                                           + (predicate.isBlank() ? "" : predicate + " and ")
                                           + support.identifier(lockField.name()) + " = ?";
                               });
    }

    /** 根 OR 在写请求中统一成为整体，后续版本或 owner 谓词只能继续收窄业务范围。 */
    private static String groupedForAdditionalPredicate(ConditionGroup where, String renderedWhere) {
        return !renderedWhere.isBlank() && where.operator() == LogicalOperator.OR && where.children().size() > 1
                ? "(" + renderedWhere + ")"
                : renderedWhere;
    }

    private Object typedValue(DynamicField field, Object value) {
        return value == null ? new SqlNullParameter(support.parameterType(field)) : value;
    }

    /** 字段只解析和编码一次，直接填充最终参数及缓存所需的列／赋值形状。 */
    private void updateAssignments(DynamicForm form, Map<String, Object> values,
                                    List<String> fields, List<Object> parameters) {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("dynamic form values must not be empty");
        }
        Map<DynamicField, Boolean> assignedFields = values.size() > 1 ? new IdentityHashMap<>(values.size()) : null;
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            DynamicField field = form.field(entry.getKey());
            if (assignedFields != null && assignedFields.put(field, Boolean.TRUE) != null) {
                throw new IllegalArgumentException("duplicate normalized dynamic update field");
            }
            String shape = ":assign";
            if (entry.getValue() instanceof ValueExpression expression) {
                FormUpdateExpressions.requirePlain(form, field);
                if (expression instanceof ValueExpression.Value value && !(value.value() instanceof ExpressionFieldValue)) {
                    parameters.add(typedValue(field, support.writeValue(field, value.value())));
                } else {
                    SqlFragment fragment = support.expression(expression, reference -> {
                        if (reference.source() != null) {
                            throw new IllegalArgumentException("UPDATE expression requires fields from the current form");
                        }
                        DynamicField source = form.field(reference.identity().name());
                        FormUpdateExpressions.requirePlain(form, source);
                        LogicalType type = source.databaseType().isArray() ? LogicalType.OTHER : source.databaseType().logicalType();
                        return new SqlExpressionCompiler.Operand(new SqlFragment(support.identifier(source.name()), List.of()), type);
                    }).fragment();
                    shape = EXPRESSION_SHAPE + fragment.sql();
                    parameters.addAll(fragment.parameters());
                }
            } else if (entry.getValue() instanceof UpdateDelta delta) {
                requireNumericUpdateField(field);
                shape = ":add";
                parameters.add(typedValue(field, support.valueCodecs.write(delta.value())));
            } else {
                parameters.add(typedValue(field, support.writeValue(field, entry.getValue())));
            }
            fields.add(field.name());
            fields.add(shape);
        }
    }

    private static void requireUnencryptedLockField(DynamicForm form, DynamicField field) {
        if (form.protections().encrypted(field.name()).isPresent()) {
            // 密文不能参与数据库原子等值比较或递增，必须在 SQL 生成前拒绝。
            throw new IllegalArgumentException("optimistic lock field must not be encrypted");
        }
    }

    private static void requireLockFieldNotAssigned(List<String> fields, DynamicField lockField) {
        for (int index = 0; index < fields.size(); index += 2) {
            if (fields.get(index).equals(lockField.name())) {
                throw new IllegalArgumentException(
                        "optimistic lock field must not also be assigned in dynamic update values: " + lockField.name());
            }
        }
    }

    private String updateExpression(DynamicForm form, String field, String shape) {
        String column = support.identifier(field);
        String value = shape.startsWith(EXPRESSION_SHAPE) ? shape.substring(EXPRESSION_SHAPE.length())
                : shape.equals(":add") ? column + " + ?" : support.valueExpression(form.field(field));
        return column + " = " + value;
    }

    private static void requireNumericUpdateField(DynamicField field) {
        if (!field.databaseType().isNumeric()) {
            throw new IllegalArgumentException("arithmetic update requires a numeric field: " + field.name());
        }
    }

}
