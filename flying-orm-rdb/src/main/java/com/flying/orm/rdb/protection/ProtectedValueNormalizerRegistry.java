package com.flying.orm.rdb.protection;

import java.text.Normalizer;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 保存有界、不可变的保护值规范化器。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class ProtectedValueNormalizerRegistry {

    private static final Pattern ID = Pattern.compile("[A-Za-z0-9._-]{1,32}");

    private final Map<String, ProtectedValueNormalizer> normalizers;

    private ProtectedValueNormalizerRegistry(Map<String, ProtectedValueNormalizer> normalizers) {
        this.normalizers = Map.copyOf(normalizers);
    }

    /** @return 包含 identity、case-fold 和 digits 的标准 registry */
    public static ProtectedValueNormalizerRegistry standard() {
        Map<String, ProtectedValueNormalizer> values = new LinkedHashMap<>();
        values.put("identity", value -> Normalizer.normalize(value, Normalizer.Form.NFC));
        values.put("case-fold", value -> Normalizer.normalize(value, Normalizer.Form.NFC)
                                                .toLowerCase(Locale.ROOT));
        values.put("digits", ProtectedValueNormalizerRegistry::digits);
        return new ProtectedValueNormalizerRegistry(values);
    }

    /**
     * 返回增加一个自定义规范化器的新 registry。
     *
     * @param id         稳定 ID
     * @param normalizer 无状态、并发安全的纯函数
     * @return 新 registry
     */
    public ProtectedValueNormalizerRegistry with(String id, ProtectedValueNormalizer normalizer) {
        String safeId = id(id);
        if (normalizers.containsKey(safeId)) {
            throw new IllegalArgumentException("duplicate protected value normalizer");
        }
        Map<String, ProtectedValueNormalizer> values = new LinkedHashMap<>(normalizers);
        values.put(safeId, Objects.requireNonNull(normalizer, "protected value normalizer must not be null"));
        return new ProtectedValueNormalizerRegistry(values);
    }

    /**
     * 执行规范化并验证结果非空、确定且不超过字段边界。
     *
     * @param id              规范化器 ID
     * @param value           原始业务文本
     * @param maxCodePoints   最大结果长度
     * @return 规范化结果
     */
    public String normalize(String id, String value, int maxCodePoints) {
        ProtectedValueNormalizer normalizer = normalizers.get(id(id));
        if (normalizer == null) {
            throw new IllegalArgumentException("protected value normalizer is not registered");
        }
        String input = Objects.requireNonNull(value, "protected search value must not be null");
        String first = normalize(normalizer, input);
        String second = normalize(normalizer, input);
        if (first == null || !first.equals(second)) {
            throw new IllegalArgumentException("protected value normalizer must be deterministic");
        }
        if (first.isEmpty()) {
            throw new IllegalArgumentException("protected normalized value must not be empty");
        }
        if (first.codePointCount(0, first.length()) > maxCodePoints) {
            throw new IllegalArgumentException("protected normalized value is too long");
        }
        return first;
    }

    /**
     * 自定义扩展可能把传入明文拼入异常；运行时只向外暴露固定分类，且不保留可能含明文的 cause。
     * {@link VirtualMachineError} 仍按 JVM 致命错误原样传播。
     */
    private static String normalize(ProtectedValueNormalizer normalizer, String input) {
        try {
            return normalizer.normalize(input);
        } catch (VirtualMachineError fatal) {
            throw fatal;
        } catch (RuntimeException | Error failure) {
            VirtualMachineError fatal = ProtectedFailureSupport.findVirtualMachineError(failure);
            if (fatal != null) {
                throw fatal;
            }
            throw new IllegalArgumentException("protected value normalizer failed");
        }
    }

    private static String digits(String value) {
        String normalized = Normalizer.normalize(value, Normalizer.Form.NFC);
        StringBuilder result = new StringBuilder(normalized.length());
        normalized.codePoints().forEach(codePoint -> {
            int digit = Character.digit(codePoint, 10);
            if (digit >= 0) {
                result.append((char) ('0' + digit));
            }
        });
        return result.toString();
    }

    private static String id(String value) {
        if (value == null || !ID.matcher(value).matches()) {
            throw new IllegalArgumentException("protected value normalizer id is invalid");
        }
        return value;
    }
}
