package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.form.spec.QuerySpec;

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
        this.fields = fields;
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

    /** 直接字段别名保留来源 codec；真正计算结果按表达式声明的结果类型解码。 */
    static FormFieldDecodingPlan queryProjection(QuerySpec spec, DynamicForm resultForm,
                                                  FormDataSqlRenderer renderer) {
        return queryProjection(spec, resultForm, renderer, spec.projections());
    }

    static FormFieldDecodingPlan queryProjection(QuerySpec spec, DynamicForm resultForm,
                                                  FormDataSqlRenderer renderer, List<String> fields) {
        Map<String, Decoding> projected = new HashMap<>();
        boolean projectedAsync = false;
        boolean projectedProtectionCpu = false;
        for (String name : fields) {
            ValueExpression expression = spec.expressions().get(FieldIdentity.of(name).key());
            DynamicField resultField = resultForm.field(name);
            Decoding decoding = expression instanceof ValueExpression.Field source
                    ? decoding(spec.form(), renderer, spec.form().field(source.identity().name()))
                    : decoding(resultForm, renderer, resultField);
            if (decoding != null) {
                projected.put(resultField.normalizedName(), decoding);
                projectedAsync |= decoding.kind().requiresAsync();
                projectedProtectionCpu |= decoding.kind() == Kind.ENCRYPTED;
            }
        }
        return projected.isEmpty() ? EMPTY
                : new FormFieldDecodingPlan(projected, projectedAsync, projectedProtectionCpu);
    }

    /** JOIN 别名只改变结果标签，解码规则仍归属于原始字段身份。 */
    static FormFieldDecodingPlan joinProjection(List<JoinProjection> projections,
                                                 DynamicForm resultForm,
                                                 FormDataSqlRenderer renderer) {
        Map<DynamicForm, FormFieldDecodingPlan> sources = new HashMap<>();
        Map<String, Decoding> projected = new HashMap<>();
        boolean projectedAsync = false;
        boolean projectedProtectionCpu = false;
        for (JoinProjection projection : projections) {
            Decoding decoding;
            if (projection.expression() == null) {
                DynamicForm source = projection.field().source().form();
                FormFieldDecodingPlan sourcePlan = sources.computeIfAbsent(source, renderer::resultDecodingPlan);
                decoding = sourcePlan.decoding(source.field(projection.field().field()));
            } else decoding = decoding(resultForm, renderer, resultForm.field(projection.alias()));
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
        List<String> safeFields = Objects.requireNonNull(
                projectedFields, "projected result fields must not be null");
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
