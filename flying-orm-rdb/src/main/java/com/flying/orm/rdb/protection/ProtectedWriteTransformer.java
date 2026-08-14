package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    /** 转换单行，保留调用方字段顺序并把每个隐藏列紧邻其业务列。 */
    ProtectedFieldRuntime.PreparedWrite prepare(DynamicForm form,
                                                 Map<String, Object> values,
                                                 DataScope scope,
                                                 ValueCodecRegistry codecs) {
        DynamicForm physical = ProtectedFormLayout.physical(form);
        String tenant = ProtectedFieldValues.tenantIdentity(form, scope, codecs);
        Map<String, Object> protectedValues = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            DynamicField field = form.field(entry.getKey());
            EncryptedFieldDefinition definition = form.protections().encrypted(field.name()).orElse(null);
            if (definition == null) {
                protectedValues.put(field.name(), entry.getValue());
                continue;
            }
            Object value = entry.getValue();
            if (value == null) {
                protectedValues.put(field.name(), null);
                addNullTokens(form, field, definition, protectedValues);
                continue;
            }
            String text = ProtectedFieldValues.encodedText(codecs, value);
            ProtectedFieldContext context = ProtectedFieldValues.context(form, field, tenant);
            protectedValues.put(field.name(), cipher.encrypt(text, context));
            if (definition.searchModes().contains(EncryptedSearchMode.EXACT)) {
                protectedValues.put(ProtectedFormLayout.exactColumn(form, field.name()),
                                    tokens.currentExactToken(text, definition, context, field.unique()));
            }
            if (definition.searchModes().contains(EncryptedSearchMode.SUFFIX)) {
                tokens.currentSuffixTokensForValue(text, definition, context)
                      .forEach((length, token) -> protectedValues.put(
                              ProtectedFormLayout.suffixColumn(form, field.name(), length), token));
            }
        }
        return new ProtectedFieldRuntime.PreparedWrite(physical, protectedValues);
    }

    /**
     * 生成批量回执使用的稳定字段身份；不返回明文，也不把随机 AES-GCM 密文当作幂等载荷。
     */
    Map<String, byte[]> receiptIdentities(DynamicForm form,
                                          Map<String, Object> values,
                                          DataScope scope,
                                          ValueCodecRegistry codecs) {
        String tenant = ProtectedFieldValues.tenantIdentity(form, scope, codecs);
        Map<String, byte[]> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            DynamicField field = form.field(entry.getKey());
            EncryptedFieldDefinition definition = form.protections().encrypted(field.name()).orElse(null);
            if (entry.getValue() == null || definition == null) {
                continue;
            }
            String text = ProtectedFieldValues.encodedText(codecs, entry.getValue());
            ProtectedFieldContext context = ProtectedFieldValues.context(form, field, tenant);
            byte[] identity = tokens.stableReceiptToken(text, context);
            result.put(field.name(), identity);
            // 回执描述逻辑载荷，而不是某次加密版本产生的内部 HMAC。否则轮换后同一 operationId 会被误判为新载荷。
            if (definition.searchModes().contains(EncryptedSearchMode.EXACT)) {
                result.put(ProtectedFormLayout.exactColumn(form, field.name()), identity.clone());
            }
            definition.suffixLengths().forEach(length -> result.put(
                    ProtectedFormLayout.suffixColumn(form, field.name(), length), identity.clone()));
        }
        return Collections.unmodifiableMap(result);
    }

    /** 为本行已声明 CONTAINS 的非空字段生成独立令牌快照。 */
    List<ProtectedFieldRuntime.ContainsFieldTokens> containsTokens(DynamicForm form,
                                                                   Map<String, Object> values,
                                                                   DataScope scope,
                                                                   ValueCodecRegistry codecs) {
        ProtectedContainsLayout.resolve(form);
        String tenant = ProtectedFieldValues.tenantIdentity(form, scope, codecs);
        List<ProtectedFieldRuntime.ContainsFieldTokens> result = new ArrayList<>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            DynamicField field = form.field(entry.getKey());
            EncryptedFieldDefinition definition = form.protections().encrypted(field.name()).orElse(null);
            if (definition == null
                    || !definition.searchModes().contains(EncryptedSearchMode.CONTAINS)) {
                continue;
            }
            if (entry.getValue() == null) {
                result.add(new ProtectedFieldRuntime.ContainsFieldTokens(
                        field.name(), ProtectedColumnNames.containsFieldTag(form.id(), field.name()), List.of()));
                continue;
            }
            String text = ProtectedFieldValues.encodedText(codecs, entry.getValue());
            ProtectedFieldContext context = ProtectedFieldValues.context(form, field, tenant);
            result.add(new ProtectedFieldRuntime.ContainsFieldTokens(
                    field.name(),
                    ProtectedColumnNames.containsFieldTag(form.id(), field.name()),
                    tokens.currentContainsTokens(text, definition, context)));
        }
        return List.copyOf(result);
    }

    private static void addNullTokens(DynamicForm form,
                                      DynamicField field,
                                      EncryptedFieldDefinition definition,
                                      Map<String, Object> values) {
        if (definition.searchModes().contains(EncryptedSearchMode.EXACT)) {
            values.put(ProtectedFormLayout.exactColumn(form, field.name()), null);
        }
        definition.suffixLengths().forEach(length -> values.put(
                ProtectedFormLayout.suffixColumn(form, field.name(), length), null));
    }
}
