package com.flying.orm.rdb.protection;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.LogicDeleteDefinition;
import com.flying.orm.core.form.TenantDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * 把公开逻辑表单转换为包含密文列和隐藏 HMAC 列的内部物理视图。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
@InternalApi
public final class ProtectedFormLayout {

    static final String CIPHERTEXT_TYPE = "PROTECTED_BINARY";
    static final String HASH_TYPE = "PROTECTED_HASH";

    private ProtectedFormLayout() {
    }

    public static DynamicForm physical(DynamicForm form) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        if (safeForm.protections().encryptedFields().isEmpty()) {
            return safeForm;
        }
        DynamicForm.Builder builder = DynamicForm.builder(safeForm.id(), safeForm.table());
        Set<String> names = new HashSet<>();
        for (DynamicField field : safeForm.fields()) {
            EncryptedFieldDefinition definition = safeForm.protections().encrypted(field.name()).orElse(null);
            if (definition == null) {
                add(builder, names, field);
                continue;
            }
            validateEncryptedField(safeForm, field, definition);
            add(builder, names, encryptedColumn(field));
            if (definition.searchModes().contains(EncryptedSearchMode.EXACT)) {
                add(builder, names, hidden(ProtectedColumnNames.exact(safeForm.id(), field.name()),
                                           field.nullable(), field.unique()));
            }
            for (int length : definition.suffixLengths()) {
                add(builder, names, hidden(ProtectedColumnNames.suffix(safeForm.id(), field.name(), length),
                                           true, false));
            }
        }
        copyTableRules(safeForm, builder);
        return builder.build();
    }

    static List<String> visibleFieldNames(DynamicForm form) {
        List<String> names = new ArrayList<>(form.fields().size());
        form.fields().forEach(field -> names.add(field.name()));
        return List.copyOf(names);
    }

    static String exactColumn(DynamicForm form, String fieldName) {
        return ProtectedColumnNames.exact(form.id(), form.field(fieldName).name());
    }

    static String suffixColumn(DynamicForm form, String fieldName, int length) {
        return ProtectedColumnNames.suffix(form.id(), form.field(fieldName).name(), length);
    }

    private static DynamicField encryptedColumn(DynamicField field) {
        return new DynamicField(field.name(), CIPHERTEXT_TYPE, false, field.nullable(), false,
                                null, null, null, field.comment(), ValueGeneration.none());
    }

    private static DynamicField hidden(String name, boolean nullable, boolean unique) {
        return new DynamicField(name, HASH_TYPE, false, nullable, unique,
                                null, null, null, null, ValueGeneration.none());
    }

    private static void add(DynamicForm.Builder builder, Set<String> names, DynamicField field) {
        if (!names.add(field.name().trim().toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("protected internal column conflicts with a form field");
        }
        builder.addField(field);
    }

    private static void copyTableRules(DynamicForm source, DynamicForm.Builder target) {
        LogicDeleteDefinition logicDelete = source.logicDelete().orElse(null);
        if (logicDelete != null) {
            target.logicDelete(logicDelete.fieldName(), logicDelete.notDeletedValue(), logicDelete.deletedValue());
        }
        TenantDefinition tenant = source.tenant().orElse(null);
        if (tenant != null) {
            target.tenant(tenant.fieldName(), tenant.strategy());
        }
    }

    private static void validateEncryptedField(DynamicForm form,
                                               DynamicField field,
                                               EncryptedFieldDefinition definition) {
        String type = field.dataType().trim().toUpperCase(Locale.ROOT);
        if (!(type.contains("CHAR") || type.contains("TEXT") || type.contains("CLOB"))) {
            throw new IllegalArgumentException("encrypted field must use a textual data type");
        }
        if (field.primaryKey() || field.generation().generated()) {
            throw new IllegalArgumentException("encrypted field must not be database-generated or a primary key");
        }
        if (field.unique() && !definition.searchModes().contains(EncryptedSearchMode.EXACT)) {
            throw new IllegalArgumentException("unique encrypted field requires exact search");
        }
        form.tenant().filter(tenant -> tenant.fieldName().equalsIgnoreCase(field.name())).ifPresent(tenant -> {
            throw new IllegalArgumentException("tenant field must not be encrypted");
        });
        form.logicDelete().filter(logic -> logic.fieldName().equalsIgnoreCase(field.name())).ifPresent(logic -> {
            throw new IllegalArgumentException("logic delete field must not be encrypted");
        });
    }
}
