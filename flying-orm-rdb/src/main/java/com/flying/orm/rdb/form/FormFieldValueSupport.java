package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.rdb.internal.value.ExpressionFieldValue;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.codec.DialectScalarValueCodec;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import com.flying.orm.rdb.json.JsonValueCodec;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.vector.VectorValueCodec;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * 表单字段写值的规范化、类型选择和批量字段顺序共用同一套无状态规则。
 *
 * @author wangr
 * @version v3.2
 */
final class FormFieldValueSupport {

    private FormFieldValueSupport() {
    }

    static FormSqlRenderSupport.WriteFields writeFields(FormSqlRenderSupport support,
                                                              DynamicForm form,
                                                              Map<String, Object> values,
                                                              Long batchRowIndex) {
        Map<String, Object> safeValues = Objects.requireNonNull(values, "dynamic form values must not be null");
        List<DynamicField> declared = form.fields();
        List<DynamicField> fields = null;
        Object[] parameters = new Object[safeValues.size()];
        SqlFragment[] fragments = null;
        Map<DynamicField, Boolean> seen = null;
        int index = 0;
        for (Map.Entry<String, Object> entry : safeValues.entrySet()) {
            DynamicField field = form.field(entry.getKey());
            requireWritableValue(support, field, entry.getValue(), batchRowIndex);
            // 声明顺序已经唯一；只有提交顺序偏离时才创建本次请求的布局和去重索引。
            if (fields == null && (index >= declared.size() || declared.get(index) != field)) {
                fields = new ArrayList<>(safeValues.size());
                seen = safeValues.size() > 1 ? new IdentityHashMap<>(safeValues.size()) : null;
                for (int previous = 0; previous < index; previous++) {
                    DynamicField prefix = declared.get(previous);
                    fields.add(prefix);
                    seen.put(prefix, Boolean.TRUE);
                }
            }
            if (fields != null) {
                if (seen != null && seen.put(field, Boolean.TRUE) != null) {
                    throw new IllegalArgumentException("duplicate normalized dynamic write field");
                }
                fields.add(field);
            }
            Object value = entry.getValue();
            if (value instanceof ValueExpression expression) {
                FormUpdateExpressions.requirePlain(form, field);
                if (expression instanceof ValueExpression.Value bound && !(bound.value() instanceof ExpressionFieldValue)) {
                    value = bound.value();
                } else {
                    if (fragments == null) fragments = new SqlFragment[safeValues.size()];
                    fragments[index++] = support.expression(expression, reference -> {
                        throw new IllegalArgumentException("INSERT values cannot reference a table row");
                    }).fragment();
                    continue;
                }
            }
            parameters[index++] = writeValue(support, field, value);
        }
        if (fields == null) fields = index == declared.size() ? declared : declared.subList(0, index);
        if (fragments == null) return new FormSqlRenderSupport.WriteFields(fields, parameters, List.of(), null);
        List<String> expressions = new ArrayList<>(fields.size());
        List<Object> bindings = new ArrayList<>();
        int[] offsets = new int[fields.size() + 1];
        for (int column = 0; column < fields.size(); column++) {
            offsets[column] = bindings.size();
            SqlFragment fragment = fragments[column];
            expressions.add(fragment == null ? support.valueExpression(fields.get(column)) : fragment.sql());
            if (fragment == null) bindings.add(parameters[column]);
            else bindings.addAll(fragment.parameters());
        }
        offsets[fields.size()] = bindings.size();
        return new FormSqlRenderSupport.WriteFields(fields, bindings.toArray(), expressions, offsets);
    }

    static Object writeValue(FormSqlRenderSupport support, DynamicField field, Object value) {
        if (value instanceof ValueExpression) {
            throw new IllegalArgumentException("SQL expression must be compiled before value binding: " + field.name());
        }
        if (value == null) {
            return null;
        }
        if (field.databaseType().logicalType() == LogicalType.VECTOR
                && !"postgresql".equalsIgnoreCase(support.dialectName)) {
            throw new IllegalArgumentException("VECTOR fields are only supported by PostgreSQL");
        }
        EntityTypeMappingRegistry.Mapping customMapping = support.customFieldCodecs.get(field);
        if (customMapping != null) {
            // descriptor 冷路径已按字段身份编译；codec 产物就是最终驱动载体。
            return customMapping.codec().write(value);
        }
        if (field.databaseType().isArray()) {
            return ArrayValueCodec.write(value, field.databaseType(), support.valueCodecs);
        }
        if (field.databaseType().logicalType() == LogicalType.VECTOR) {
            return VectorValueCodec.write(value, field.length());
        }
        if (isJson(field)) {
            return JsonValueCodec.write(value);
        }
        if (LargeObjectValueCodec.isLargeObjectDataType(field.databaseType())) {
            return LargeObjectValueCodec.write(value, field.databaseType(), support.dialectName);
        }
        Object encoded = DialectScalarValueCodec.write(
                value, field.databaseType(), support.dialectName,
                support.nativeBoolean, support.valueCodecs);
        if (encoded instanceof UUID uuid && support.parameterType(field) == String.class) {
            // 应用 codec 优先；只有 codec 仍保留 UUID 时才执行跨方言 VARCHAR 默认回退。
            return uuid.toString();
        }
        return encoded;
    }

    static Class<?> scalarParameterType(FormSqlRenderSupport support, DynamicField field) {
        return DialectScalarValueCodec.parameterType(
                field.databaseType(), support.dialectName, support.nativeBoolean);
    }

    static boolean isJson(DynamicField field) {
        return !field.databaseType().isArray()
                && field.databaseType().logicalType() == LogicalType.JSON;
    }

    static boolean isTextBackedOffsetTime(FormSqlRenderSupport support, DynamicField field) {
        return !field.databaseType().isArray() && field.databaseType().logicalType() == LogicalType.OFFSET_TIME
                && ("mysql".equalsIgnoreCase(support.dialectName)
                    || "oracle".equalsIgnoreCase(support.dialectName)
                    || "sqlserver".equalsIgnoreCase(support.dialectName));
    }

    private static void requireWritableValue(FormSqlRenderSupport support,
                                             DynamicField field,
                                             Object value,
                                             Long batchRowIndex) {
        if (value instanceof UpdateDelta) {
            if (batchRowIndex != null) {
                throw new IllegalArgumentException("batch write row [" + batchRowIndex + "] field ["
                                                           + field.name() + "] does not allow update delta");
            }
            throw new IllegalArgumentException(
                    "update delta is only valid in an update SET clause: " + field.name());
        }
        if ("sqlserver".equalsIgnoreCase(support.dialectName)
                && field.generation().strategy() == ValueGeneration.Strategy.IDENTITY) {
            throw new IllegalArgumentException(
                    "SQL Server identity field must be omitted from write values: " + field.name());
        }
    }
}
