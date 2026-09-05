package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * 为显式、可恢复的密钥轮换任务识别旧版本密文并恢复待重写的逻辑值。
 *
 * <p>本类型不扫描数据库、不保存进度，也不控制事务。上层按稳定主键游标读取物理密文后调用
 * {@link #valuesNeedingReprotection(DynamicForm, Map, DataScope, ValueCodecRegistry)}；返回空 Map 表示该行已经使用
 * current 密钥。非空结果应通过普通 {@code FormClient.update(WriteSpec)} 写回，使密文、EXACT/SUFFIX 和 CONTAINS
 * 侧索引继续由同一原子工作单元维护。上层仍只管理版本化主密钥环。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
public final class ProtectedFieldReprotection {

    private final ProtectedFieldKeyRing keys;
    private final ProtectedFieldCipher cipher;

    private ProtectedFieldReprotection(ProtectedFieldKeyRing keys) {
        this.keys = Objects.requireNonNull(keys, "protected field key ring must not be null");
        this.cipher = new ProtectedFieldCipher(keys);
    }

    /**
     * 创建不拥有密钥环生命周期的轮换协作者。
     *
     * @param keys 仍包含旧 readable 版本的密钥环
     * @return 可并发复用的轮换协作者
     */
    public static ProtectedFieldReprotection create(ProtectedFieldKeyRing keys) {
        return new ProtectedFieldReprotection(keys);
    }

    /**
     * 读取有界密文信封中的非敏感密钥版本。
     *
     * @param ciphertext 数据库物理密文
     * @return 密钥版本
     */
    public String ciphertextVersion(Object ciphertext) {
        return ProtectedFieldEnvelope.keyVersion(ProtectedFieldValues.binary(
                Objects.requireNonNull(ciphertext, "protected ciphertext must not be null")));
    }

    /**
     * 为显式的“旧明文列到新密文列”迁移提取尚未完成的逻辑值。
     *
     * <p>调用方必须分别传入可信的旧明文投影和目标密文投影。目标字段已有合法、可读版本的密文时幂等跳过；
     * 目标缺失时才返回旧明文。返回值应通过普通 {@code FormClient.update(WriteSpec)} 写入，使密文和全部搜索索引
     * 仍由统一事务维护。本方法不猜测同一列中的值究竟是明文还是密文。</p>
     *
     * @param form                 带显式保护声明的逻辑表单
     * @param legacyPlaintextValues 可信迁移查询读取的旧明文字段和值
     * @param targetPhysicalValues 目标密文字段和值；应来自独立目标列或目标表
     * @return 尚需迁移的逻辑字段值；空 Map 表示该行可幂等跳过
     */
    public Map<String, Object> valuesNeedingPlaintextMigration(DynamicForm form,
                                                                Map<String, Object> legacyPlaintextValues,
                                                                Map<String, Object> targetPhysicalValues) {
        return valuesNeedingPlaintextMigration(form, legacyPlaintextValues, targetPhysicalValues,
                DataScope.none(), ValueCodecRegistry.standard());
    }

    /**
     * 使用与目标写入一致的可信租户范围和 codec，认证已有密文并提取尚需迁移的旧明文。
     *
     * @param form 带保护声明的逻辑表单
     * @param legacyPlaintextValues 可信迁移查询读取的旧明文字段和值
     * @param targetPhysicalValues 可信迁移查询读取的目标密文字段和值
     * @param scope 与目标写入一致的可信租户范围
     * @param codecs 与目标写入一致的值 codec
     * @return 尚需迁移的逻辑字段值；空 Map 表示该行可幂等跳过
     */
    public Map<String, Object> valuesNeedingPlaintextMigration(DynamicForm form,
                                                                Map<String, Object> legacyPlaintextValues,
                                                                Map<String, Object> targetPhysicalValues,
                                                                DataScope scope,
                                                                ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        Map<String, Object> legacy = Objects.requireNonNull(
                legacyPlaintextValues, "legacy plaintext values must not be null");
        Map<String, Object> target = Objects.requireNonNull(
                targetPhysicalValues, "target physical values must not be null");
        DataScope safeScope = Objects.requireNonNull(scope, "data scope must not be null");
        ValueCodecRegistry safeCodecs = Objects.requireNonNull(codecs, "value codec registry must not be null");
        String tenant = null;
        Map<String, Object> result = new LinkedHashMap<>();
        int protectedFieldCount = safeForm.protections().encryptedFields().size();
        ValueLookup targetLookup = new ValueLookup(target, protectedFieldCount);
        ValueLookup legacyLookup = null;
        for (String fieldName : safeForm.protections().encryptedFields().keySet()) {
            DynamicField field = safeForm.field(fieldName);
            Object ciphertext = targetLookup.value(field.name());
            if (ciphertext != null) {
                if (tenant == null) {
                    tenant = ProtectedFieldValues.tenantIdentity(safeForm, safeScope, safeCodecs);
                }
                try {
                    cipher.verify(ProtectedFieldValues.binary(ciphertext),
                                  ProtectedFieldValues.context(safeForm, field, tenant));
                    continue;
                } catch (ProtectedFieldException | IllegalArgumentException invalidCiphertext) {
                    if (legacyLookup == null) {
                        legacyLookup = new ValueLookup(legacy, protectedFieldCount);
                    }
                    Object plaintext = legacyLookup.value(field.name());
                    if (plaintext == null) {
                        throw invalidCiphertext;
                    }
                    result.put(field.name(), copyArray(plaintext));
                    continue;
                }
            }
            if (legacyLookup == null) {
                legacyLookup = new ValueLookup(legacy, protectedFieldCount);
            }
            Object plaintext = legacyLookup.value(field.name());
            if (plaintext != null) {
                result.put(field.name(), copyArray(plaintext));
            }
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * 只解密仍使用 readable 旧版本的字段；current 版本和 null 字段会被幂等跳过。
     *
     * @param form           带显式保护声明的逻辑表单
     * @param physicalValues 可信迁移查询返回的物理字段和值
     * @param scope          与原写入相同的租户范围
     * @param codecs         统一值 codec
     * @return 可直接交给普通更新规格的逻辑字段值；空 Map 表示无需重写
     */
    public Map<String, Object> valuesNeedingReprotection(DynamicForm form,
                                                          Map<String, Object> physicalValues,
                                                          DataScope scope,
                                                          ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        Map<String, Object> safeValues = Objects.requireNonNull(
                physicalValues, "protected physical values must not be null");
        String tenant = ProtectedFieldValues.tenantIdentity(
                safeForm, Objects.requireNonNull(scope, "data scope must not be null"),
                Objects.requireNonNull(codecs, "value codec registry must not be null"));
        Map<String, Object> result = new LinkedHashMap<>();
        ValueLookup values = new ValueLookup(
                safeValues, safeForm.protections().encryptedFields().size());
        for (String fieldName : safeForm.protections().encryptedFields().keySet()) {
            DynamicField field = safeForm.field(fieldName);
            Object value = values.value(field.name());
            if (value == null) {
                continue;
            }
            byte[] envelope = ProtectedFieldValues.binary(value);
            if (keys.currentVersion().equals(ProtectedFieldEnvelope.keyVersion(envelope))) {
                cipher.verify(envelope, ProtectedFieldValues.context(safeForm, field, tenant));
                continue;
            }
            result.put(field.name(), cipher.decrypt(
                    envelope, ProtectedFieldValues.context(safeForm, field, tenant)));
        }
        return Collections.unmodifiableMap(result);
    }

    private static final class ValueLookup {

        private final Map<String, Object> values;
        private final TreeMap<String, Match> matches;

        private ValueLookup(Map<String, Object> values, int protectedFieldCount) {
            if (protectedFieldCount <= 1) {
                this.values = values;
                this.matches = null;
                return;
            }
            this.values = null;
            this.matches = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                String field = entry.getKey();
                if (field == null) {
                    continue;
                }
                Match match = matches.get(field);
                if (match == null) {
                    matches.put(field, new Match(entry.getValue()));
                } else {
                    match.ambiguous = true;
                }
            }
        }

        private Object value(String field) {
            if (matches == null) {
                return ProtectedFieldReprotection.value(values, field);
            }
            Match match = matches.get(field);
            if (match == null) {
                return null;
            }
            if (match.ambiguous) {
                throw new IllegalArgumentException("protected migration column is ambiguous");
            }
            return match.value;
        }
    }

    private static final class Match {

        private final Object value;
        private boolean ambiguous;

        private Match(Object value) {
            this.value = value;
        }
    }

    private static Object value(Map<String, Object> values, String field) {
        boolean matched = false;
        Object result = null;
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (!field.equalsIgnoreCase(entry.getKey())) {
                continue;
            }
            if (matched) {
                throw new IllegalArgumentException("protected migration column is ambiguous");
            }
            matched = true;
            result = entry.getValue();
        }
        return result;
    }

    private static Object copyArray(Object value) {
        if (value == null || !value.getClass().isArray()) {
            return value;
        }
        int length = Array.getLength(value);
        Object copy = Array.newInstance(value.getClass().getComponentType(), length);
        System.arraycopy(value, 0, copy, 0, length);
        return copy;
    }
}
