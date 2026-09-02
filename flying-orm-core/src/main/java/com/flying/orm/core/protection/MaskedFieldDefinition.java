package com.flying.orm.core.protection;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 保存一个字段的通用业务脱敏声明。
 *
 * @param policy  masking policy 稳定 ID
 * @param prefix  保留的前缀 code point 数
 * @param suffix  保留的后缀 code point 数
 * @param display 字段声明的默认展示方式
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public record MaskedFieldDefinition(String policy,
                                    int prefix,
                                    int suffix,
                                    SensitiveDisplayMode display) {

    private static final Pattern EXTENSION_ID = Pattern.compile("[A-Za-z0-9._-]{1,32}");

    /** 完成字段声明校验。 */
    public MaskedFieldDefinition {
        if (policy == null || !EXTENSION_ID.matcher(policy).matches()) {
            throw new IllegalArgumentException("masking policy id is invalid");
        }
        if (prefix < 0 || suffix < 0) {
            throw new IllegalArgumentException("masking retained length must not be negative");
        }
        display = Objects.requireNonNull(display, "sensitive display mode must not be null");
        if (display == SensitiveDisplayMode.DECLARED) {
            throw new IllegalArgumentException("field display declaration must be MASKED or FULL");
        }
    }

    /** @param policy masking policy 稳定 ID；@return 构建器 */
    public static Builder builder(String policy) {
        return new Builder(policy);
    }

    /** 构建字段脱敏声明。 */
    public static final class Builder {

        private final String policy;
        private int prefix;
        private int suffix;
        private SensitiveDisplayMode display = SensitiveDisplayMode.MASKED;

        private Builder(String policy) {
            this.policy = policy;
        }

        /** @return 当前构建器 */
        public Builder prefix(int value) {
            this.prefix = value;
            return this;
        }

        /** @return 当前构建器 */
        public Builder suffix(int value) {
            this.suffix = value;
            return this;
        }

        /** @return 当前构建器 */
        public Builder display(SensitiveDisplayMode value) {
            this.display = value;
            return this;
        }

        /** @return 不可变字段脱敏声明 */
        public MaskedFieldDefinition build() {
            return new MaskedFieldDefinition(policy, prefix, suffix, display);
        }
    }
}
