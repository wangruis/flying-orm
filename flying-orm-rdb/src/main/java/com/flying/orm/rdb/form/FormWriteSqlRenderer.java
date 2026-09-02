package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import com.flying.orm.rdb.lock.OptimisticLockMode;
import com.flying.orm.rdb.lock.OptimisticLockOptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * 动态表单单条写入的内部渲染器。
 *
 * <p>它只负责把 insert、update 和 delete 组织成参数化 SQL；字段识别、值转换、条件编译、计划缓存和
 * 必须带 where 的安全校验仍统一交给 {@link FormSqlRenderSupport}。对象在门面创建时生成，之后不保存请求级状态，
 * 因而可以被多个线程并发复用。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class FormWriteSqlRenderer {

    private final FormSqlRenderSupport support;

    FormWriteSqlRenderer(FormSqlRenderSupport support) {
        this.support = Objects.requireNonNull(support, "form SQL render support must not be null");
    }

    /**
     * 渲染单条 insert，字段顺序沿用通用写入字段校验的结果。
     */
    SqlRequest insert(DynamicForm form, Map<String, Object> values) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        List<FormSqlRenderSupport.FieldValue> fieldValues = support.writeFields(safeForm, values, null);
        List<Object> parameters = new ArrayList<>(fieldValues.size());
        List<String> fields = new ArrayList<>(fieldValues.size());
        for (FormSqlRenderSupport.FieldValue fieldValue : fieldValues) {
            fields.add(fieldValue.field().name());
            parameters.add(typedValue(fieldValue.field(), fieldValue.value()));
        }
        return support.request("insert", safeForm, fields, FormSqlRenderSupport.ConditionSql.none(), "", "", "",
                               parameters, () -> {
                                   StringJoiner columns = new StringJoiner(", ");
                                   StringJoiner placeholders = new StringJoiner(", ");
                                   for (FormSqlRenderSupport.FieldValue fieldValue : fieldValues) {
                                       columns.add(support.identifier(fieldValue.field().name()));
                                       placeholders.add(support.valueExpression(fieldValue.field()));
                                   }
                                   return "insert into " + support.identifier(safeForm.table()) + " (" + columns
                                           + ") values (" + placeholders + ")";
                               });
    }

    /**
     * 渲染带业务 where 的更新。
     */
    SqlRequest update(DynamicForm form, Map<String, Object> values, ConditionGroup where) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        List<UpdateAssignment> fieldValues = updateAssignments(safeForm, values);
        FormSqlRenderSupport.ConditionSql whereFragment = support.requiredWhere(safeForm, where, "update");
        List<Object> parameters = new ArrayList<>(fieldValues.size() + whereFragment.parameters().size());
        List<String> fields = new ArrayList<>(fieldValues.size());
        for (UpdateAssignment fieldValue : fieldValues) {
            fields.add(fieldValue.field().name() + (fieldValue.arithmetic() ? ":add" : ":assign"));
            parameters.add(typedValue(fieldValue.field(), fieldValue.value()));
        }
        parameters.addAll(whereFragment.parameters());
        return support.request("update", safeForm, fields, whereFragment, "", "", "",
                               parameters, () -> {
                                   StringJoiner sets = new StringJoiner(", ");
                                   fieldValues.forEach(fieldValue -> sets.add(updateExpression(fieldValue)));
                                   return "update " + support.identifier(safeForm.table()) + " set " + sets + " where "
                                           + groupedForAdditionalPredicate(where, whereFragment.sql());
                               });
    }

    /**
     * 渲染乐观锁更新。版本字段的 INCREMENT 和 ASSIGN 语义保持在 SQL 和参数顺序中。
     */
    SqlRequest update(DynamicForm form,
                      Map<String, Object> values,
                      ConditionGroup where,
                      OptimisticLockOptions lock) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        DynamicField lockField = support.field(safeForm, safeLock.field());
        requireUnencryptedLockField(safeForm, lockField);
        List<UpdateAssignment> fieldValues = updateAssignments(safeForm, values);
        requireLockFieldNotAssigned(fieldValues, lockField);
        FormSqlRenderSupport.ConditionSql whereFragment = support.requiredWhere(safeForm, where, "update");
        List<Object> parameters = new ArrayList<>(fieldValues.size() + whereFragment.parameters().size() + 2);
        List<String> fields = new ArrayList<>(fieldValues.size() + 1);
        for (UpdateAssignment fieldValue : fieldValues) {
            fields.add(fieldValue.field().name() + (fieldValue.arithmetic() ? ":add" : ":assign"));
            parameters.add(typedValue(fieldValue.field(), fieldValue.value()));
        }
        if (safeLock.mode() == OptimisticLockMode.INCREMENT) {
            fields.add(lockField.name() + ":lock-increment");
        } else {
            fields.add(lockField.name() + ":lock-assign");
            parameters.add(typedValue(lockField, support.writeValue(lockField, safeLock.nextValue())));
        }
        parameters.addAll(whereFragment.parameters());
        parameters.add(typedValue(lockField, support.writeValue(lockField, safeLock.expectedValue())));
        return support.request("update-optimistic", safeForm, fields, whereFragment, "", "", "",
                               parameters, () -> {
                                   StringJoiner sets = new StringJoiner(", ");
                                   fieldValues.forEach(fieldValue -> sets.add(updateExpression(fieldValue)));
                                   if (safeLock.mode() == OptimisticLockMode.INCREMENT) {
                                       sets.add(support.identifier(lockField.name()) + " = "
                                                        + support.identifier(lockField.name()) + " + 1");
                                   } else {
                                       sets.add(support.identifier(lockField.name()) + " = ?");
                                   }
                                   return "update " + support.identifier(safeForm.table()) + " set " + sets + " where "
                                           + groupedForAdditionalPredicate(where, whereFragment.sql()) + " and "
                                           + support.identifier(lockField.name()) + " = ?";
                               });
    }

    /**
     * 根据已经渲染好的乐观锁更新请求固定后续批量行的参数类型和绑定布局。
     */
    BatchUpdatePlan optimisticUpdatePlan(DynamicForm form,
                                         Map<String, Object> values,
                                         ConditionGroup where,
                                         OptimisticLockOptions lock,
                                         SqlRequest request) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        DynamicField lockField = support.field(safeForm, safeLock.field());
        SqlRequest safeRequest = Objects.requireNonNull(request, "optimistic update request must not be null");
        List<Object> parameters = safeRequest.parameters();
        List<Class<?>> parameterTypes = new ArrayList<>(parameters.size());
        // 首行已完成字段、锁和 where 校验；只恢复类型布局，不再次编码已准备好的值。
        for (String fieldName : values.keySet()) {
            parameterTypes.add(support.parameterType(support.field(safeForm, fieldName)));
        }
        if (safeLock.mode() == OptimisticLockMode.ASSIGN) {
            parameterTypes.add(support.parameterType(lockField));
        }
        // SET（含可选 next version）之后、expected version 之前是已编码的 WHERE 参数。
        for (int index = parameterTypes.size(); index < parameters.size() - 1; index++) {
            Object value = parameters.get(index);
            parameterTypes.add(value == null ? Object.class : value.getClass());
        }
        parameterTypes.add(support.parameterType(lockField));
        return new BatchUpdatePlan(safeRequest.statement(), parameterTypes);
    }

    /**
     * 渲染带必填业务 where 的物理删除。
     */
    SqlRequest delete(DynamicForm form, ConditionGroup where) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        FormSqlRenderSupport.ConditionSql whereFragment = support.requiredWhere(safeForm, where, "delete");
        return support.request("delete", safeForm, List.of(), whereFragment, "", "", "",
                               whereFragment.parameters(), () -> "delete from "
                                       + support.identifier(safeForm.table()) + " where " + whereFragment.sql());
    }

    /**
     * 渲染带乐观锁版本条件的删除，版本字段只参与 where，不会被修改。
     */
    SqlRequest delete(DynamicForm form, ConditionGroup where, OptimisticLockOptions lock) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        DynamicField lockField = support.field(safeForm, safeLock.field());
        requireUnencryptedLockField(safeForm, lockField);
        FormSqlRenderSupport.ConditionSql whereFragment = support.requiredWhere(safeForm, where, "delete");
        List<Object> parameters = new ArrayList<>(whereFragment.parameters().size() + 1);
        parameters.addAll(whereFragment.parameters());
        parameters.add(typedValue(lockField, support.writeValue(lockField, safeLock.expectedValue())));
        return support.request("delete-optimistic", safeForm, List.of(lockField.name()), whereFragment, "", "", "",
                               parameters, () -> "delete from " + support.identifier(safeForm.table())
                                       + " where " + groupedForAdditionalPredicate(where, whereFragment.sql()) + " and "
                                       + support.identifier(lockField.name()) + " = ?");
    }

    /** 根 OR 在写请求中统一成为整体，后续版本或 owner 谓词只能继续收窄业务范围。 */
    private static String groupedForAdditionalPredicate(ConditionGroup where, String renderedWhere) {
        return where.operator() == LogicalOperator.OR && where.children().size() > 1
                ? "(" + renderedWhere + ")"
                : renderedWhere;
    }

    private Object typedValue(DynamicField field, Object value) {
        return value == null ? new SqlNullParameter(support.parameterType(field)) : value;
    }

    private List<UpdateAssignment> updateAssignments(DynamicForm form, Map<String, Object> values) {
        Map<String, Object> safeValues = Objects.requireNonNull(values, "dynamic form values must not be null");
        if (safeValues.isEmpty()) {
            throw new IllegalArgumentException("dynamic form values must not be empty");
        }
        List<UpdateAssignment> assignments = new ArrayList<>(safeValues.size());
        Map<String, String> sourceNames = new HashMap<>(Math.max(16, safeValues.size() * 2));
        for (Map.Entry<String, Object> entry : safeValues.entrySet()) {
            DynamicField field = support.field(form, entry.getKey());
            String previousName = sourceNames.putIfAbsent(field.normalizedName(), entry.getKey());
            if (previousName != null) {
                throw new IllegalArgumentException("duplicate normalized dynamic update field");
            }
            if (entry.getValue() instanceof UpdateDelta delta) {
                requireNumericUpdateField(field);
                assignments.add(new UpdateAssignment(field, support.valueCodecs.write(delta.value()), true));
            } else {
                assignments.add(new UpdateAssignment(field, support.writeValue(field, entry.getValue()), false));
            }
        }
        return assignments;
    }

    private static void requireUnencryptedLockField(DynamicForm form, DynamicField field) {
        if (form.protections().encrypted(field.name()).isPresent()) {
            // 密文不能参与数据库原子等值比较或递增，必须在 SQL 生成前拒绝。
            throw new IllegalArgumentException("optimistic lock field must not be encrypted");
        }
    }

    private static void requireLockFieldNotAssigned(List<UpdateAssignment> assignments, DynamicField lockField) {
        if (assignments.stream().anyMatch(assignment -> assignment.field().normalizedName()
                                                               .equals(lockField.normalizedName()))) {
            throw new IllegalArgumentException(
                    "optimistic lock field must not also be assigned in dynamic update values: " + lockField.name());
        }
    }

    private String updateExpression(UpdateAssignment assignment) {
        String column = support.identifier(assignment.field().name());
        return assignment.arithmetic() ? column + " = " + column + " + ?"
                : column + " = " + support.valueExpression(assignment.field());
    }

    private static void requireNumericUpdateField(DynamicField field) {
        if (!field.databaseType().isNumeric()) {
            throw new IllegalArgumentException("arithmetic update requires a numeric field: " + field.name());
        }
    }

    private record UpdateAssignment(DynamicField field, Object value, boolean arithmetic) {
    }
}
