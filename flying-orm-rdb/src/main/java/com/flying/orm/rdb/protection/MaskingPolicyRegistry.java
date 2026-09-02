package com.flying.orm.rdb.protection;

import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import com.flying.orm.core.protection.MaskedFieldDefinition;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 保存不可变、可并发共享的通用 masking policy。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class MaskingPolicyRegistry {

    private static final Pattern ID = Pattern.compile("[A-Za-z0-9._-]{1,32}");

    private final Map<String, MaskingPolicy> policies;

    private MaskingPolicyRegistry(Map<String, MaskingPolicy> policies) {
        this.policies = Map.copyOf(policies);
    }

    /** @return 包含通用内建策略的 registry */
    public static MaskingPolicyRegistry standard() {
        Map<String, MaskingPolicy> values = new LinkedHashMap<>();
        values.put("full", (value, definition) -> stars(codePoints(value).length));
        values.put("partial", MaskingPolicyRegistry::partial);
        values.put("email", MaskingPolicyRegistry::email);
        values.put("person-name", (value, definition) -> keep(value, 1, 0));
        values.put("address", (value, definition) -> keep(value, Math.min(6, codePoints(value).length), 0));
        values.put("bank-card", (value, definition) -> keep(value, 0, Math.min(4, codePoints(value).length)));
        return new MaskingPolicyRegistry(values);
    }

    /** @return 增加自定义策略后的新 registry */
    public MaskingPolicyRegistry with(String id, MaskingPolicy policy) {
        String safeId = id(id);
        if (policies.containsKey(safeId)) {
            throw new IllegalArgumentException("duplicate masking policy");
        }
        Map<String, MaskingPolicy> values = new LinkedHashMap<>(policies);
        values.put(safeId, Objects.requireNonNull(policy, "masking policy must not be null"));
        return new MaskingPolicyRegistry(values);
    }

    /** @return null 或脱敏后的有界文本 */
    public String mask(String value, MaskedFieldDefinition definition) {
        if (value == null) {
            return null;
        }
        MaskedFieldDefinition safeDefinition = Objects.requireNonNull(
                definition, "masked field definition must not be null");
        MaskingPolicy policy = policies.get(id(safeDefinition.policy()));
        if (policy == null) {
            throw new IllegalArgumentException("masking policy is not registered");
        }
        String result = mask(policy, value, safeDefinition);
        if (result == null || result.codePointCount(0, result.length()) > value.codePointCount(0, value.length()) + 8) {
            throw new IllegalArgumentException("masking policy returned an invalid result");
        }
        return result;
    }

    /**
     * 自定义策略持有解密后的完整值，不能让其异常消息或 cause 把明文带到 ORM 公共错误边界。
     * {@link VirtualMachineError} 仍按 JVM 致命错误原样传播。
     */
    private static String mask(MaskingPolicy policy, String value, MaskedFieldDefinition definition) {
        try {
            return policy.mask(value, definition);
        } catch (VirtualMachineError fatal) {
            throw fatal;
        } catch (RuntimeException | Error failure) {
            VirtualMachineError fatal = findVirtualMachineError(failure);
            if (fatal != null) {
                throw fatal;
            }
            throw new IllegalArgumentException("masking policy failed");
        }
    }

    private static String partial(String value, MaskedFieldDefinition definition) {
        return keep(value, definition.prefix(), definition.suffix());
    }

    private static String email(String value, MaskedFieldDefinition definition) {
        int separator = value.indexOf('@');
        if (separator <= 0 || separator == value.length() - 1) {
            return keep(value, definition.prefix(), definition.suffix());
        }
        int[] local = codePoints(value.substring(0, separator));
        String visible = local.length == 0 ? "" : new String(local, 0, 1);
        return visible + "***@" + value.substring(separator + 1);
    }

    private static String keep(String value, int prefix, int suffix) {
        int[] points = codePoints(value);
        int safePrefix = Math.min(prefix, points.length);
        int safeSuffix = Math.min(suffix, points.length - safePrefix);
        if (safePrefix + safeSuffix >= points.length) {
            return stars(points.length);
        }
        return new String(points, 0, safePrefix)
                + stars(Math.max(3, points.length - safePrefix - safeSuffix))
                + new String(points, points.length - safeSuffix, safeSuffix);
    }

    private static int[] codePoints(String value) {
        return value.codePoints().toArray();
    }

    private static String stars(int count) {
        return "*".repeat(count);
    }

    private static String id(String value) {
        if (value == null || !ID.matcher(value).matches()) {
            throw new IllegalArgumentException("masking policy id is invalid");
        }
        return value;
    }
}
