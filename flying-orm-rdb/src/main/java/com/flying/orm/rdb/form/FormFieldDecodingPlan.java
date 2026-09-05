package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import com.flying.orm.rdb.codec.OffsetTimeValueCodec;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 查询级字段解码计划；字段类型只识别一次，结果布局再按该计划绑定投影列。 */
final class FormFieldDecodingPlan {

    static final FormFieldDecodingPlan EMPTY = new FormFieldDecodingPlan(Map.of(), false, false);

    private final Map<String, Decoding> fields;
    private final boolean requiresAsync;
    private final boolean requiresProtectionCpu;

    private FormFieldDecodingPlan(Map<String, Decoding> fields,
                                  boolean requiresAsync,
                                  boolean requiresProtectionCpu) {
        this.fields = Map.copyOf(fields);
        this.requiresAsync = requiresAsync;
        this.requiresProtectionCpu = requiresProtectionCpu;
    }

    static FormFieldDecodingPlan compile(DynamicForm form, FormDataSqlRenderer renderer) {
        Map<String, Decoding> fields = null;
        boolean requiresAsync = false;
        boolean requiresProtectionCpu = false;
        for (DynamicField field : form.fields()) {
            Decoding decoding = decoding(form, renderer, field);
            if (decoding != null) {
                if (fields == null) {
                    fields = new HashMap<>();
                }
                fields.put(field.normalizedName(), decoding);
                requiresAsync |= decoding.kind().requiresAsync();
                requiresProtectionCpu |= decoding.kind() == Kind.ENCRYPTED;
            }
        }
        return fields == null
                ? EMPTY : new FormFieldDecodingPlan(fields, requiresAsync, requiresProtectionCpu);
    }

    boolean isEmpty() {
        return fields.isEmpty();
    }

    /** JOIN 别名只改变结果标签，解码规则仍归属于原始字段身份。 */
    static FormFieldDecodingPlan joinProjection(JoinQuerySpec spec,
                                                 DynamicForm resultForm,
                                                 FormDataSqlRenderer renderer) {
        Map<DynamicForm, FormFieldDecodingPlan> sources = new HashMap<>();
        Map<String, Decoding> projected = new HashMap<>();
        boolean projectedAsync = false;
        boolean projectedProtectionCpu = false;
        for (JoinProjection projection : spec.projections()) {
            DynamicForm source = projection.field().source().form();
            FormFieldDecodingPlan sourcePlan = sources.computeIfAbsent(source, renderer::resultDecodingPlan);
            Decoding decoding = sourcePlan.decoding(source.field(projection.field().field()));
            if (decoding != null) {
                projected.put(resultForm.field(projection.alias()).normalizedName(), decoding);
                projectedAsync |= decoding.kind().requiresAsync();
                projectedProtectionCpu |= decoding.kind() == Kind.ENCRYPTED;
            }
        }
        return projected.isEmpty()
                ? EMPTY : new FormFieldDecodingPlan(projected, projectedAsync, projectedProtectionCpu);
    }

    int size() {
        return fields.size();
    }

    boolean requiresAsync() {
        return requiresAsync;
    }

    boolean requiresProtectionCpu() {
        return requiresProtectionCpu;
    }

    FormFieldDecodingPlan projected(DynamicForm form, List<String> projectedFields) {
        Objects.requireNonNull(form, "dynamic form must not be null");
        List<String> safeFields = List.copyOf(Objects.requireNonNull(
                projectedFields, "projected result fields must not be null"));
        if (isEmpty() || safeFields.isEmpty()) {
            return EMPTY;
        }
        Map<String, Decoding> projected = new HashMap<>(Math.min(fields.size(), safeFields.size()));
        boolean projectedAsync = false;
        boolean projectedProtectionCpu = false;
        for (String fieldName : safeFields) {
            DynamicField field = form.findField(fieldName).orElse(null);
            Decoding decoding = field == null ? null : decoding(field);
            if (decoding != null) {
                projected.put(field.normalizedName(), decoding);
                projectedAsync |= decoding.kind().requiresAsync();
                projectedProtectionCpu |= decoding.kind() == Kind.ENCRYPTED;
            }
        }
        return projected.isEmpty()
                ? EMPTY : new FormFieldDecodingPlan(projected, projectedAsync, projectedProtectionCpu);
    }

    Decoding decoding(DynamicField field) {
        return fields.get(field.normalizedName());
    }

    private static Decoding decoding(DynamicForm form,
                                     FormDataSqlRenderer renderer,
                                     DynamicField field) {
        if (form.protections().encrypted(field.name()).isPresent()) {
            return Decoding.of(Kind.ENCRYPTED);
        }
        EntityTypeMappingRegistry.Mapping customMapping = renderer.customFieldMapping(field);
        if (customMapping != null) {
            return LargeObjectValueCodec.isLargeObjectDataType(field.databaseType())
                    ? Decoding.custom(Kind.CUSTOM_LARGE_OBJECT, customMapping)
                    : Decoding.custom(Kind.CUSTOM, customMapping);
        }
        if (!field.databaseType().isArray() && field.databaseType().logicalType() == LogicalType.JSON) {
            return Decoding.of(Kind.JSON);
        }
        if (field.databaseType().isArray()) {
            return Decoding.array(ArrayValueCodec.parameterType(field.databaseType()));
        }
        if (field.databaseType().logicalType() == LogicalType.VECTOR) {
            return Decoding.of(Kind.VECTOR);
        }
        if (OffsetTimeValueCodec.isOffsetTimeDataType(field.databaseType())) {
            return Decoding.of(Kind.OFFSET_TIME);
        }
        FormScalarReadPlan scalar = renderer.scalarReadPlan(field);
        if (scalar != null) {
            return Decoding.scalar(scalar);
        }
        return LargeObjectValueCodec.isLargeObjectDataType(field.databaseType())
                ? Decoding.of(Kind.LARGE_OBJECT) : null;
    }

    enum Kind {
        ENCRYPTED,
        JSON,
        ARRAY,
        VECTOR,
        OFFSET_TIME,
        SCALAR,
        LARGE_OBJECT,
        CUSTOM,
        CUSTOM_LARGE_OBJECT;

        boolean requiresAsync() {
            return this == ENCRYPTED || this == LARGE_OBJECT || this == CUSTOM_LARGE_OBJECT;
        }
    }

    record Decoding(Kind kind,
                    FormScalarReadPlan scalar,
                    Class<?> arrayType,
                    EntityTypeMappingRegistry.Mapping customMapping) {

        Decoding {
            Objects.requireNonNull(kind, "field decoding kind must not be null");
            if ((kind == Kind.SCALAR) != (scalar != null)) {
                throw new IllegalArgumentException("scalar decoding plan must match its kind");
            }
            if ((kind == Kind.ARRAY) != (arrayType != null)) {
                throw new IllegalArgumentException("array decoding plan must match its kind");
            }
            boolean custom = kind == Kind.CUSTOM || kind == Kind.CUSTOM_LARGE_OBJECT;
            if (custom != (customMapping != null)) {
                throw new IllegalArgumentException("custom decoding plan must match its kind");
            }
        }

        static Decoding of(Kind kind) {
            return new Decoding(kind, null, null, null);
        }

        static Decoding scalar(FormScalarReadPlan scalar) {
            return new Decoding(Kind.SCALAR,
                                Objects.requireNonNull(scalar, "scalar read plan must not be null"), null, null);
        }

        static Decoding array(Class<?> arrayType) {
            return new Decoding(Kind.ARRAY, null,
                                Objects.requireNonNull(arrayType, "array target type must not be null"), null);
        }

        static Decoding custom(Kind kind, EntityTypeMappingRegistry.Mapping mapping) {
            return new Decoding(kind, null, null,
                                Objects.requireNonNull(mapping, "custom entity mapping must not be null"));
        }
    }
}
