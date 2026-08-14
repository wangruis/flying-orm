package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
        String tenant = ProtectedFieldValues.tenantIdentity(form, scope, codecs);
        Map<Integer, Object> replacements = new HashMap<>();
        boolean canonicalNamesRequired = false;
        for (int index = 0; index < row.columnCount(); index++) {
            DynamicField field = form.findField(row.columnName(index)).orElse(null);
            if (field != null && !field.name().equals(row.columnName(index))) {
                canonicalNamesRequired = true;
            }
            if (field == null || form.protections().encrypted(field.name()).isEmpty()) {
                continue;
            }
            Object value = row.value(index);
            if (value != null) {
                replacements.put(index, cipher.decrypt(
                        ProtectedFieldValues.binary(value), ProtectedFieldValues.context(form, field, tenant)));
            }
        }
        DynamicRow transformed = row.withValues(replacements);
        if (canonicalNamesRequired) {
            transformed = transformed.renameColumns(name -> form.findField(name)
                    .map(DynamicField::name)
                    .orElse(name));
        }
        return masking.transform(form, transformed, displayMode);
    }
}
