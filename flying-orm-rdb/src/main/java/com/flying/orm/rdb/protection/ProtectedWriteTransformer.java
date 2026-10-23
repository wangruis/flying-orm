package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 把一行逻辑写入值转换为密文和声明过的盲索引列。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class ProtectedWriteTransformer {

    private final ProtectedFieldCipher cipher;
    private final ProtectedSearchTokenService tokens;

    ProtectedWriteTransformer(ProtectedFieldCipher cipher, ProtectedSearchTokenService tokens) {
        this.cipher = Objects.requireNonNull(cipher, "protected field cipher must not be null");
        this.tokens = Objects.requireNonNull(tokens, "protected search token service must not be null");
    }

    Plan plan(DynamicForm form,
              DynamicForm physicalForm,
              DataScope scope,
              ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        DynamicForm safePhysicalForm = Objects.requireNonNull(
                physicalForm, "physical form must not be null");
        ValueCodecRegistry safeCodecs = Objects.requireNonNull(
                codecs, "value codec registry must not be null");
        String tenant = ProtectedFieldValues.tenantIdentity(safeForm, scope, safeCodecs);
        Map<String, FieldPlan> fields = new LinkedHashMap<>();
        for (DynamicField field : safeForm.fields()) {
            EncryptedFieldDefinition definition = safeForm.protections().encrypted(field.name()).orElse(null);
            if (definition != null) {
                fields.put(field.name(), FieldPlan.compile(safeForm, field, definition, tenant));
            }
        }
        return new Plan(safeForm, safePhysicalForm, safeCodecs, fields);
    }

    final class Plan {

        private final DynamicForm form;
        private final DynamicForm physicalForm;
        private final ValueCodecRegistry codecs;
        private final Map<String, FieldPlan> fields;

        private Plan(DynamicForm form,
                     DynamicForm physicalForm,
                     ValueCodecRegistry codecs,
                     Map<String, FieldPlan> fields) {
            this.form = form;
            this.physicalForm = physicalForm;
            this.codecs = codecs;
            this.fields = fields;
        }

        ProtectedFieldRuntime.PreparedWrite prepare(
                Map<String, Object> values, Consumer<ProtectedFieldRuntime.ContainsFieldTokens> contains) {
            Map<String, Object> safeValues = Objects.requireNonNull(
                    values, "dynamic form values must not be null");
            Map<String, Object> protectedValues = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : safeValues.entrySet()) {
                DynamicField field = form.field(entry.getKey());
                if (protectedValues.containsKey(field.name())) {
                    throw new IllegalArgumentException("duplicate normalized protected write field");
                }
                FieldPlan fieldPlan = fields.get(field.name());
                if (fieldPlan == null) {
                    protectedValues.put(
                            field.name(), ProtectedFieldRuntime.PreparedWrite.snapshotValue(field, entry.getValue()));
                    continue;
                }
                Object value = entry.getValue();
                if (value == null) {
                    protectedValues.put(field.name(), null);
                    fieldPlan.addNullTokens(protectedValues);
                    if (contains != null && fieldPlan.containsFieldTag() != null) {
                        contains.accept(containsTokens(field.name(), fieldPlan, null));
                    }
                    continue;
                }
                String text = ProtectedFieldValues.encodedText(codecs, value);
                protectedValues.put(field.name(), cipher.encrypt(text, fieldPlan.context()));
                if (contains != null && fieldPlan.containsFieldTag() != null) {
                    contains.accept(containsTokens(field.name(), fieldPlan, text));
                }
                if (fieldPlan.exactColumn() != null) {
                    protectedValues.put(fieldPlan.exactColumn(), tokens.currentExactToken(
                            text, fieldPlan.definition(), fieldPlan.context(), field.unique()));
                }
                if (!fieldPlan.suffixColumns().isEmpty()) {
                    tokens.currentSuffixTokensForValue(
                            text, fieldPlan.definition(), fieldPlan.context()).forEach(
                                    (length, token) -> protectedValues.put(
                                            fieldPlan.suffixColumns().get(length), token));
                }
            }
            return ProtectedFieldRuntime.PreparedWrite.owned(physicalForm, protectedValues);
        }

        List<ProtectedFieldRuntime.ContainsFieldTokens> containsTokens(Map<String, Object> values) {
            Map<String, Object> safeValues = Objects.requireNonNull(
                    values, "dynamic form values must not be null");
            List<ProtectedFieldRuntime.ContainsFieldTokens> result = new ArrayList<>();
            for (Map.Entry<String, Object> entry : safeValues.entrySet()) {
                DynamicField field = form.field(entry.getKey());
                FieldPlan fieldPlan = fields.get(field.name());
                if (fieldPlan == null || fieldPlan.containsFieldTag() == null) {
                    continue;
                }
                String text = entry.getValue() == null ? null
                        : ProtectedFieldValues.encodedText(codecs, entry.getValue());
                result.add(containsTokens(field.name(), fieldPlan, text));
            }
            return List.copyOf(result);
        }

        private ProtectedFieldRuntime.ContainsFieldTokens containsTokens(
                String name, FieldPlan field, String text) {
            return new ProtectedFieldRuntime.ContainsFieldTokens(name, field.containsFieldTag(), text == null
                    ? List.of() : tokens.currentContainsTokens(text, field.definition(), field.context()));
        }
    }

    private record FieldPlan(EncryptedFieldDefinition definition,
                             ProtectedFieldContext context,
                             String exactColumn,
                             Map<Integer, String> suffixColumns,
                             String containsFieldTag) {

        private static FieldPlan compile(DynamicForm form,
                                         DynamicField field,
                                         EncryptedFieldDefinition definition,
                                         String tenant) {
            String exactColumn = definition.searchModes().contains(EncryptedSearchMode.EXACT)
                    ? ProtectedFormLayout.exactColumn(form, field.name()) : null;
            Map<Integer, String> suffixColumns = new LinkedHashMap<>();
            definition.suffixLengths().forEach(length -> suffixColumns.put(
                    length, ProtectedFormLayout.suffixColumn(form, field.name(), length)));
            String containsFieldTag = definition.searchModes().contains(EncryptedSearchMode.CONTAINS)
                    ? ProtectedColumnNames.containsFieldTag(form.id(), field.name()) : null;
            return new FieldPlan(
                    definition,
                    ProtectedFieldValues.context(form, field, tenant),
                    exactColumn,
                    Map.copyOf(suffixColumns),
                    containsFieldTag);
        }

        private void addNullTokens(Map<String, Object> values) {
            if (exactColumn != null) {
                values.put(exactColumn, null);
            }
            suffixColumns.values().forEach(column -> values.put(column, null));
        }
    }
}
