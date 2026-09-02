package com.flying.orm.rdb.protection;

import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** 生成字段和租户隔离的 EXACT/SUFFIX HMAC-SHA-256 token。 */
final class ProtectedSearchTokenService {

    private static final byte[] DERIVATION_SALT =
            "flying-orm/protected-search/v1".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_CONTAINS_TOKENS = 4096;

    private final ProtectedFieldKeyRing keys;
    private final ProtectedValueNormalizerRegistry normalizers;

    ProtectedSearchTokenService(ProtectedFieldKeyRing keys,
                                ProtectedValueNormalizerRegistry normalizers) {
        this.keys = Objects.requireNonNull(keys, "protected field key ring must not be null");
        this.normalizers = Objects.requireNonNull(normalizers, "protected normalizer registry must not be null");
    }

    byte[] currentExactToken(String value,
                             EncryptedFieldDefinition definition,
                             ProtectedFieldContext context) {
        return currentExactToken(value, definition, context, false);
    }

    byte[] currentExactToken(String value,
                             EncryptedFieldDefinition definition,
                             ProtectedFieldContext context,
                             boolean unique) {
        requireMode(definition, EncryptedSearchMode.EXACT);
        String normalized = normalize(value, definition);
        return unique
                ? token(keys.uniqueSearchKey(), "exact", normalized, context)
                : token(keys.currentVersion(), "exact", normalized, context);
    }

    /**
     * 为批量回执生成与随机密文和加密密钥轮换无关的稳定载荷身份。
     *
     * <p>这里使用写入前的完整编码文本而不是搜索规范化结果，避免两个仅搜索等价、实际明文不同的值
     * 被误认为同一个批量载荷。身份值只进入回执摘要，不会绑定到业务表。</p>
     */
    byte[] stableReceiptToken(String encodedValue, ProtectedFieldContext context) {
        return token(keys.uniqueSearchKey(),
                     "receipt",
                     Objects.requireNonNull(encodedValue, "protected receipt value must not be null"),
                     Objects.requireNonNull(context, "protected field context must not be null"));
    }

    List<byte[]> exactQueryTokens(String value,
                                  EncryptedFieldDefinition definition,
                                  ProtectedFieldContext context) {
        return exactQueryTokens(value, definition, context, false);
    }

    List<byte[]> exactQueryTokens(String value,
                                  EncryptedFieldDefinition definition,
                                  ProtectedFieldContext context,
                                  boolean unique) {
        requireMode(definition, EncryptedSearchMode.EXACT);
        String normalized = normalize(value, definition);
        if (unique) {
            return List.of(token(keys.uniqueSearchKey(), "exact", normalized, context));
        }
        List<byte[]> result = new ArrayList<>();
        for (String version : keys.versionsInSearchOrder()) {
            result.add(token(version, "exact", normalized, context));
        }
        return List.copyOf(result);
    }

    byte[] currentSuffixToken(String value,
                              EncryptedFieldDefinition definition,
                              ProtectedFieldContext context) {
        requireMode(definition, EncryptedSearchMode.SUFFIX);
        String normalized = normalize(value, definition);
        int length = normalized.codePointCount(0, normalized.length());
        if (!definition.suffixLengths().contains(length)) {
            throw new IllegalArgumentException("protected suffix length is not declared");
        }
        return token(keys.currentVersion(), "suffix/" + length, normalized, context);
    }

    Map<Integer, byte[]> currentSuffixTokensForValue(String value,
                                                      EncryptedFieldDefinition definition,
                                                      ProtectedFieldContext context) {
        requireMode(definition, EncryptedSearchMode.SUFFIX);
        String normalized = normalize(value, definition);
        int[] points = normalized.codePoints().toArray();
        Map<Integer, byte[]> result = new LinkedHashMap<>();
        for (int length : definition.suffixLengths()) {
            if (points.length < length) {
                // 声明的是可查询后缀长度，不是业务值最小长度；短值在对应隐藏列保存 NULL。
                result.put(length, null);
                continue;
            }
            String suffix = new String(points, points.length - length, length);
            result.put(length, token(keys.currentVersion(), "suffix/" + length, suffix, context));
        }
        return Collections.unmodifiableMap(result);
    }

    SuffixQuery suffixQuery(String value,
                            EncryptedFieldDefinition definition,
                            ProtectedFieldContext context) {
        requireMode(definition, EncryptedSearchMode.SUFFIX);
        String normalized = normalize(value, definition);
        int length = normalized.codePointCount(0, normalized.length());
        if (!definition.suffixLengths().contains(length)) {
            throw new IllegalArgumentException("protected suffix length is not declared");
        }
        List<byte[]> result = new ArrayList<>();
        for (String version : keys.versionsInSearchOrder()) {
            result.add(token(version, "suffix/" + length, normalized, context));
        }
        return new SuffixQuery(length, result);
    }

    /** 为一条新写入生成 current 密钥版本的去重 trigram token。 */
    List<byte[]> currentContainsTokens(String value,
                                       EncryptedFieldDefinition definition,
                                       ProtectedFieldContext context) {
        requireMode(definition, EncryptedSearchMode.CONTAINS);
        List<String> trigrams = containsTrigrams(normalize(value, definition), definition, false);
        return containsTokens(keys.currentVersion(), trigrams, context);
    }

    /** 为 current 与 readable 版本分别生成候选查询 token，禁止跨版本拼接局部命中。 */
    ContainsQuery containsQuery(String value,
                                EncryptedFieldDefinition definition,
                                ProtectedFieldContext context) {
        requireMode(definition, EncryptedSearchMode.CONTAINS);
        String normalized = normalize(value, definition);
        List<String> trigrams = containsTrigrams(normalized, definition, true);
        List<ContainsTokenGroup> groups = new ArrayList<>();
        for (String version : keys.versionsInSearchOrder()) {
            groups.add(new ContainsTokenGroup(version, containsTokens(version, trigrams, context)));
        }
        return new ContainsQuery(normalized, groups, trigrams.size());
    }

    boolean matchesContains(String value,
                            EncryptedFieldDefinition definition,
                            String normalizedQuery) {
        String normalizedValue = normalize(
                Objects.requireNonNull(value, "protected contains candidate value must not be null"),
                definition);
        return normalizedValue.contains(Objects.requireNonNull(
                normalizedQuery, "protected contains normalized query must not be null"));
    }

    private List<byte[]> containsTokens(String version,
                                        List<String> trigrams,
                                        ProtectedFieldContext context) {
        if (trigrams.isEmpty()) {
            return List.of();
        }
        byte[] masterKey = keys.masterKey(version);
        try {
            byte[] tokenKey = HkdfSha256.derive(masterKey,
                                                 DERIVATION_SALT,
                                                 context.derivationInfo("contains"),
                                                 32);
            try {
                Mac mac = Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(tokenKey, "HmacSHA256"));
                List<byte[]> result = new ArrayList<>(trigrams.size());
                for (String trigram : trigrams) {
                    result.add(mac.doFinal(trigram.getBytes(StandardCharsets.UTF_8)));
                }
                return List.copyOf(result);
            } catch (GeneralSecurityException error) {
                throw new IllegalStateException("HmacSHA256 is required by Java 21", error);
            } finally {
                Arrays.fill(tokenKey, (byte) 0);
            }
        } finally {
            Arrays.fill(masterKey, (byte) 0);
        }
    }

    private static List<String> containsTrigrams(String normalized,
                                                  EncryptedFieldDefinition definition,
                                                  boolean query) {
        int[] points = normalized.codePoints().toArray();
        if (query && points.length < definition.containsMinLength()) {
            throw new IllegalArgumentException("protected contains search value is too short");
        }
        if (points.length < 3) {
            return List.of();
        }
        Set<String> unique = new LinkedHashSet<>();
        for (int index = 0; index <= points.length - 3; index++) {
            unique.add(new String(points, index, 3));
            if (unique.size() > MAX_CONTAINS_TOKENS) {
                throw new IllegalArgumentException("protected contains token limit exceeded");
            }
        }
        return List.copyOf(unique);
    }

    private String normalize(String value, EncryptedFieldDefinition definition) {
        return normalizers.normalize(definition.normalizer(), value, definition.maxNormalizedLength());
    }

    private byte[] token(String version,
                         String purpose,
                         String normalized,
                         ProtectedFieldContext context) {
        byte[] masterKey = keys.masterKey(version);
        return token(masterKey, purpose, normalized, context);
    }

    private byte[] token(byte[] masterKey,
                         String purpose,
                         String normalized,
                         ProtectedFieldContext context) {
        try {
            byte[] tokenKey = HkdfSha256.derive(masterKey,
                                               DERIVATION_SALT,
                                               context.derivationInfo(purpose),
                                               32);
            try {
                Mac mac = Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(tokenKey, "HmacSHA256"));
                return mac.doFinal(normalized.getBytes(StandardCharsets.UTF_8));
            } catch (GeneralSecurityException error) {
                throw new IllegalStateException("HmacSHA256 is required by Java 21", error);
            } finally {
                Arrays.fill(tokenKey, (byte) 0);
            }
        } finally {
            Arrays.fill(masterKey, (byte) 0);
        }
    }

    private static void requireMode(EncryptedFieldDefinition definition, EncryptedSearchMode mode) {
        if (!Objects.requireNonNull(definition, "encrypted field definition must not be null")
                    .searchModes().contains(mode)) {
            throw new IllegalArgumentException("protected search mode is not declared");
        }
    }

    /** 规范化后的后缀长度与按密钥版本排序的查询 token。 */
    record SuffixQuery(int length, List<byte[]> tokens) {
        SuffixQuery {
            if (length < 1) {
                throw new IllegalArgumentException("protected suffix length must be positive");
            }
            tokens = List.copyOf(Objects.requireNonNull(tokens, "protected suffix tokens must not be null"));
        }
    }

    /** 同一密钥版本下必须全部命中的 contains token 组。 */
    record ContainsTokenGroup(String keyVersion, List<byte[]> tokens) {
        ContainsTokenGroup {
            keyVersion = Objects.requireNonNull(keyVersion, "protected contains key version must not be null");
            tokens = List.copyOf(Objects.requireNonNull(tokens, "protected contains tokens must not be null"));
        }
    }

    /** 按版本分组的候选查询令牌及每组必须命中的去重令牌数。 */
    record ContainsQuery(String normalizedValue, List<ContainsTokenGroup> groups, int distinctTokenCount) {
        ContainsQuery {
            normalizedValue = Objects.requireNonNull(
                    normalizedValue, "protected contains normalized value must not be null");
            groups = List.copyOf(Objects.requireNonNull(groups, "protected contains groups must not be null"));
            if (groups.isEmpty() || distinctTokenCount < 1) {
                throw new IllegalArgumentException("protected contains query must contain tokens");
            }
            if (groups.stream().anyMatch(group -> group.tokens.size() != distinctTokenCount)) {
                throw new IllegalArgumentException("protected contains token groups must have equal sizes");
            }
        }
    }
}
