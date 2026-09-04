package com.flying.orm.core.form;

import com.flying.orm.core.protection.FieldProtectionRegistry;

import java.util.Map;
import java.util.Objects;

/** 集中校验动态表单的跨字段约束，让只读表单本身只保留模型和公开行为。 */
final class DynamicFormValidation {

    private DynamicFormValidation() {
    }

    static LogicDeleteDefinition logicDelete(
            LogicDeleteDefinition definition, Map<String, DynamicField> fieldsByName) {
        if (!fieldsByName.containsKey(definition.identity().key())) {
            throw new IllegalArgumentException("logic delete field does not exist in form");
        }
        return definition;
    }

    static TenantDefinition tenant(
            TenantDefinition definition, Map<String, DynamicField> fieldsByName) {
        if (!fieldsByName.containsKey(definition.identity().key())) {
            throw new IllegalArgumentException("tenant field does not exist in form");
        }
        return definition;
    }

    static FieldProtectionRegistry protections(
            FieldProtectionRegistry registry,
            Map<String, DynamicField> fieldsByName,
            TenantDefinition tenant,
            LogicDeleteDefinition logicDelete) {
        FieldProtectionRegistry safeRegistry = Objects.requireNonNull(
                registry, "field protection registry must not be null");
        validateFieldPresence(safeRegistry, fieldsByName);
        validateControlFields(safeRegistry, fieldsByName, tenant, logicDelete);
        validateDataTypes(safeRegistry, fieldsByName);
        return safeRegistry;
    }

    private static void validateFieldPresence(
            FieldProtectionRegistry registry, Map<String, DynamicField> fieldsByName) {
        for (String fieldName : registry.encryptedFields().keySet()) {
            requireField(fieldsByName, fieldName);
        }
        for (String fieldName : registry.maskedFields().keySet()) {
            requireField(fieldsByName, fieldName);
        }
    }

    private static void validateControlFields(
            FieldProtectionRegistry registry,
            Map<String, DynamicField> fieldsByName,
            TenantDefinition tenant,
            LogicDeleteDefinition logicDelete) {
        for (String fieldName : registry.encryptedFields().keySet()) {
            DynamicField field = fieldsByName.get(fieldName);
            if (field.primaryKey()
                    || tenant != null && fieldName.equals(tenant.identity().key())
                    || logicDelete != null && fieldName.equals(logicDelete.identity().key())) {
                // ORM 会自动生成这些等值条件，随机密文无法可靠参与比较，所以必须在表单发布前拒绝。
                throw new IllegalArgumentException("encrypted field must not be an ORM control field");
            }
        }
    }

    private static void validateDataTypes(
            FieldProtectionRegistry registry, Map<String, DynamicField> fieldsByName) {
        for (String fieldName : registry.encryptedFields().keySet()) {
            requireTextual(fieldsByName.get(fieldName));
        }
        for (String fieldName : registry.maskedFields().keySet()) {
            requireTextual(fieldsByName.get(fieldName));
        }
    }

    private static void requireField(Map<String, DynamicField> fieldsByName, String fieldName) {
        if (!fieldsByName.containsKey(fieldName)) {
            throw new IllegalArgumentException("protected field does not exist in form");
        }
    }

    private static void requireTextual(DynamicField field) {
        if (!field.databaseType().isTextual()) {
            // 加密和脱敏都按稳定文本编码工作，不能把不支持的类型留到首次查询时才失败。
            throw new IllegalArgumentException("protected field must use a textual data type");
        }
    }
}
