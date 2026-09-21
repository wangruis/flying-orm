package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.codec.DialectScalarValueCodec;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import com.flying.orm.rdb.codec.OffsetTimeValueCodec;
import com.flying.orm.rdb.json.JsonValueCodec;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.vector.VectorValueCodec;

import java.util.ArrayList;
import java.util.HashMap;
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

    static List<FormSqlRenderSupport.FieldValue> writeFields(FormSqlRenderSupport support,
                                                              DynamicForm form,
                                                              Map<String, Object> values,
                                                              Long batchRowIndex) {
        Map<String, Object> safeValues = Objects.requireNonNull(values, "dynamic form values must not be null");
        if (safeValues.isEmpty()) {
            throw new IllegalArgumentException("dynamic form values must not be empty");
        }
        List<FormSqlRenderSupport.FieldValue> fieldValues = new ArrayList<>(safeValues.size());
        Map<String, String> sourceNames = new HashMap<>(Math.max(16, safeValues.size() * 2));
        for (Map.Entry<String, Object> entry : safeValues.entrySet()) {
            DynamicField field = support.field(form, entry.getKey());
            requireWritableValue(support, field, entry.getValue(), batchRowIndex);
            String previousName = sourceNames.putIfAbsent(field.normalizedName(), entry.getKey());
            if (previousName != null) {
                throw new IllegalArgumentException("duplicate normalized dynamic write field");
            }
            fieldValues.add(new FormSqlRenderSupport.FieldValue(
                    field, writeValue(support, field, entry.getValue())));
        }
        return fieldValues;
    }

    static Object writeValue(FormSqlRenderSupport support, DynamicField field, Object value) {
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
        if (OffsetTimeValueCodec.isOffsetTimeDataType(field.databaseType())) {
            return OffsetTimeValueCodec.write(value, field.databaseType(), support.dialectName);
        }
        if (isJson(field)) {
            return JsonValueCodec.write(value);
        }
        if (LargeObjectValueCodec.isLargeObjectDataType(field.databaseType())) {
            return LargeObjectValueCodec.write(value, field.databaseType(), support.dialectName);
        }
        if (scalarParameterType(support, field) != Object.class) {
            return DialectScalarValueCodec.write(
                    value, field.databaseType(), support.dialectName,
                    support.nativeBoolean, support.valueCodecs);
        }
        Object encoded = support.valueCodecs.write(value);
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
        return OffsetTimeValueCodec.isOffsetTimeDataType(field.databaseType())
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
