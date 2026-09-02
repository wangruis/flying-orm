package com.flying.orm.core.protection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 保存一个字段的加密与保护搜索声明。
 *
 * <p>该类型只描述能力，不保存密钥、密文或业务值，可以安全放入表单和实体元数据缓存。</p>
 *
 * @param searchModes        显式启用的搜索方式
 * @param normalizer         规范化器稳定 ID
 * @param suffixLengths      允许检索的 Unicode code point 后缀长度
 * @param maxNormalizedLength 规范化结果的最大 code point 数
 * @param containsMinLength  contains 查询的最小 code point 数
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public record EncryptedFieldDefinition(Set<EncryptedSearchMode> searchModes,
                                       String normalizer,
                                       List<Integer> suffixLengths,
                                       int maxNormalizedLength,
                                       int containsMinLength) {

    private static final int MAX_NORMALIZED_LENGTH = 65_536;
    private static final int MAX_SUFFIX_LENGTH_COUNT = 32;
    private static final Pattern EXTENSION_ID = Pattern.compile("[A-Za-z0-9._-]{1,32}");

    /** 完成不可变快照和跨属性校验。 */
    public EncryptedFieldDefinition {
        Objects.requireNonNull(searchModes, "encrypted search modes must not be null");
        EnumSet<EncryptedSearchMode> copiedModes = searchModes.isEmpty()
                ? EnumSet.noneOf(EncryptedSearchMode.class)
                : EnumSet.copyOf(searchModes);
        searchModes = Set.copyOf(copiedModes);
        if (normalizer == null || !EXTENSION_ID.matcher(normalizer).matches()) {
            throw new IllegalArgumentException("protected value normalizer id is invalid");
        }
        Objects.requireNonNull(suffixLengths, "encrypted suffix lengths must not be null");
        suffixLengths = suffixLengths.stream()
                                     .map(length -> Objects.requireNonNull(
                                             length, "encrypted suffix length must not be null"))
                                     .peek(length -> requirePositive(length, "encrypted suffix length"))
                                     .distinct()
                                     .sorted()
                                     .toList();
        if (suffixLengths.size() > MAX_SUFFIX_LENGTH_COUNT) {
            throw new IllegalArgumentException("encrypted suffix length count exceeds the safe limit");
        }
        if (copiedModes.contains(EncryptedSearchMode.SUFFIX) != !suffixLengths.isEmpty()) {
            throw new IllegalArgumentException("suffix search requires declared suffix lengths");
        }
        if (maxNormalizedLength < 1 || maxNormalizedLength > MAX_NORMALIZED_LENGTH) {
            throw new IllegalArgumentException("protected normalized length is out of range");
        }
        if (suffixLengths.stream().anyMatch(length -> length > maxNormalizedLength)) {
            throw new IllegalArgumentException("encrypted suffix length exceeds normalized length limit");
        }
        if (copiedModes.contains(EncryptedSearchMode.CONTAINS)) {
            if (containsMinLength < 3) {
                throw new IllegalArgumentException("contains search minimum length must be at least three");
            }
            if (containsMinLength > maxNormalizedLength) {
                throw new IllegalArgumentException("contains search minimum length exceeds normalized length limit");
            }
        }
    }

    /** @return 使用安全默认值的构建器；默认启用精确搜索 */
    public static Builder builder() {
        return new Builder();
    }

    private static void requirePositive(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    /** 构建字段加密声明。 */
    public static final class Builder {

        private Set<EncryptedSearchMode> searchModes = EnumSet.of(EncryptedSearchMode.EXACT);
        private String normalizer = "identity";
        private List<Integer> suffixLengths = List.of();
        private int maxNormalizedLength = 4096;
        private int containsMinLength = 3;

        private Builder() {
        }

        /** @return 当前构建器 */
        public Builder searchModes(EncryptedSearchMode... modes) {
            Objects.requireNonNull(modes, "encrypted search modes must not be null");
            this.searchModes = modes.length == 0
                    ? EnumSet.noneOf(EncryptedSearchMode.class)
                    : EnumSet.copyOf(Arrays.asList(modes.clone()));
            return this;
        }

        /** @return 当前构建器 */
        public Builder normalizer(String normalizer) {
            this.normalizer = normalizer;
            return this;
        }

        /** @return 当前构建器 */
        public Builder suffixLengths(int... lengths) {
            Objects.requireNonNull(lengths, "encrypted suffix lengths must not be null");
            List<Integer> values = new ArrayList<>(lengths.length);
            for (int length : lengths.clone()) {
                values.add(length);
            }
            this.suffixLengths = List.copyOf(values);
            return this;
        }

        /** @return 当前构建器 */
        public Builder maxNormalizedLength(int value) {
            this.maxNormalizedLength = value;
            return this;
        }

        /** @return 当前构建器 */
        public Builder containsMinLength(int value) {
            this.containsMinLength = value;
            return this;
        }

        /** @return 不可变字段加密声明 */
        public EncryptedFieldDefinition build() {
            return new EncryptedFieldDefinition(searchModes,
                                                normalizer,
                                                suffixLengths,
                                                maxNormalizedLength,
                                                containsMinLength);
        }
    }
}
