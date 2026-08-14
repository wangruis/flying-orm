package com.flying.orm.core.protection;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 按规范化字段名保存加密和业务脱敏声明。
 *
 * <p>registry 不推断字段用途；只有明确写入 builder 的字段才会被视为受保护字段。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class FieldProtectionRegistry {

    private final Map<String, EncryptedFieldDefinition> encrypted;
    private final Map<String, MaskedFieldDefinition> masked;

    private FieldProtectionRegistry(Map<String, EncryptedFieldDefinition> encrypted,
                                    Map<String, MaskedFieldDefinition> masked) {
        this.encrypted = Map.copyOf(encrypted);
        this.masked = Map.copyOf(masked);
    }

    /** @return 空 registry */
    public static FieldProtectionRegistry empty() {
        return new FieldProtectionRegistry(Map.of(), Map.of());
    }

    /** @return 新 registry 构建器 */
    public static Builder builder() {
        return new Builder();
    }

    /** @param fieldName 字段名；@return 加密声明 */
    public Optional<EncryptedFieldDefinition> encrypted(String fieldName) {
        return Optional.ofNullable(encrypted.get(normalize(fieldName)));
    }

    /** @param fieldName 字段名；@return 业务脱敏声明 */
    public Optional<MaskedFieldDefinition> masked(String fieldName) {
        return Optional.ofNullable(masked.get(normalize(fieldName)));
    }

    /** @param fieldName 字段名；@return 是否存在任意保护声明 */
    public boolean protectedField(String fieldName) {
        String key = normalize(fieldName);
        return encrypted.containsKey(key) || masked.containsKey(key);
    }

    /** @return 是否没有任何字段保护声明 */
    public boolean isEmpty() {
        return encrypted.isEmpty() && masked.isEmpty();
    }

    /** @return 不可修改的加密声明映射 */
    public Map<String, EncryptedFieldDefinition> encryptedFields() {
        return encrypted;
    }

    /** @return 不可修改的脱敏声明映射 */
    public Map<String, MaskedFieldDefinition> maskedFields() {
        return masked;
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("protected field name must not be blank");
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    /** 构建不可变字段保护 registry。 */
    public static final class Builder {

        private final Map<String, EncryptedFieldDefinition> encrypted = new LinkedHashMap<>();
        private final Map<String, MaskedFieldDefinition> masked = new LinkedHashMap<>();

        private Builder() {
        }

        /** @return 当前构建器 */
        public Builder encrypted(String fieldName, EncryptedFieldDefinition definition) {
            String key = normalize(fieldName);
            if (encrypted.putIfAbsent(key, Objects.requireNonNull(
                    definition, "encrypted field definition must not be null")) != null) {
                throw new IllegalArgumentException("duplicate encrypted field declaration");
            }
            return this;
        }

        /** @return 当前构建器 */
        public Builder masked(String fieldName, MaskedFieldDefinition definition) {
            String key = normalize(fieldName);
            if (masked.putIfAbsent(key, Objects.requireNonNull(
                    definition, "masked field definition must not be null")) != null) {
                throw new IllegalArgumentException("duplicate masked field declaration");
            }
            return this;
        }

        /** @return 不可变字段保护 registry */
        public FieldProtectionRegistry build() {
            return new FieldProtectionRegistry(encrypted, masked);
        }
    }
}
