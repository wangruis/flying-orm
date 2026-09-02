package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * 在结果边界完成受保护列解密和已声明字段的业务脱敏。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class ProtectedResultTransformer {

    private final ProtectedFieldCipher cipher;
    private final MaskedFieldResultTransformer masking;

    ProtectedResultTransformer(ProtectedFieldCipher cipher, MaskedFieldResultTransformer masking) {
        this.cipher = Objects.requireNonNull(cipher, "protected field cipher must not be null");
        this.masking = Objects.requireNonNull(masking, "masked field result transformer must not be null");
    }

    /** 解密业务可见列后应用本次查询的显示策略。 */
    DynamicRow transform(DynamicForm form,
                         DynamicRow row,
                         DataScope scope,
                         SensitiveDisplayMode displayMode,
                         ValueCodecRegistry codecs) {
        return plan(form, scope, displayMode, codecs).transform(row);
    }

    ResultPlan plan(DynamicForm form,
                    DataScope scope,
                    SensitiveDisplayMode displayMode,
                    ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        SensitiveDisplayMode safeDisplayMode = Objects.requireNonNull(
                displayMode, "sensitive display mode must not be null");
        String tenant = ProtectedFieldValues.tenantIdentity(safeForm, scope, codecs);
        return new ResultPlan(safeForm, tenant, safeDisplayMode);
    }

    final class ResultPlan {

        private final DynamicForm form;
        private final String tenant;
        private final SensitiveDisplayMode displayMode;

        private ResultPlan(DynamicForm form, String tenant, SensitiveDisplayMode displayMode) {
            this.form = form;
            this.tenant = tenant;
            this.displayMode = displayMode;
        }

        DynamicRow transform(DynamicRow row) {
            DynamicRow safeRow = Objects.requireNonNull(row, "dynamic row must not be null");
            BoundResultPlan plan = safeRow.mappingBinding(form, () -> bind(form, safeRow));
            Map<Integer, Object> replacements = null;
            for (ProtectedColumn column : plan.columns()) {
                Object value = safeRow.value(column.index());
                if (column.encrypted() && value != null) {
                    value = cipher.decrypt(
                            ProtectedFieldValues.binary(value),
                            ProtectedFieldValues.context(form, column.field(), tenant));
                }
                Object protectedValue = masking.transformValue(
                        value, column.masking(), displayMode);
                if (column.encrypted() && value != null || protectedValue != value) {
                    if (replacements == null) {
                        replacements = new HashMap<>();
                    }
                    replacements.put(column.index(), protectedValue);
                }
            }
            DynamicRow transformed = replacements == null ? safeRow : safeRow.withValues(replacements);
            if (plan.canonicalNames() != null) {
                transformed = transformed.renameColumnsBound(plan.canonicalNames());
            }
            return transformed;
        }
    }

    private static BoundResultPlan bind(DynamicForm form, DynamicRow row) {
        List<ProtectedColumn> columns = new ArrayList<>(form.protections().encryptedFields().size()
                + form.protections().maskedFields().size());
        boolean canonicalNamesRequired = false;
        for (int index = 0; index < row.columnCount(); index++) {
            String columnName = row.columnName(index);
            DynamicField field = form.findField(columnName).orElse(null);
            if (field == null) {
                continue;
            }
            canonicalNamesRequired |= !field.name().equals(columnName);
            boolean encrypted = form.protections().encrypted(field.name()).isPresent();
            MaskedFieldDefinition masking = form.protections().masked(field.name()).orElse(null);
            if (encrypted || masking != null) {
                columns.add(new ProtectedColumn(index, field, encrypted, masking));
            }
        }
        UnaryOperator<String> canonicalNames = canonicalNamesRequired
                ? name -> form.findField(name).map(DynamicField::name).orElse(name)
                : null;
        return new BoundResultPlan(List.copyOf(columns), canonicalNames);
    }

    private record BoundResultPlan(List<ProtectedColumn> columns,
                                   UnaryOperator<String> canonicalNames) {
    }

    private record ProtectedColumn(int index,
                                   DynamicField field,
                                   boolean encrypted,
                                   MaskedFieldDefinition masking) {
    }
}
