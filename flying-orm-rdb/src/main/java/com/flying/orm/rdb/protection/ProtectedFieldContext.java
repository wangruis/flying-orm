package com.flying.orm.rdb.protection;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 提供字段密码用途隔离所需的稳定上下文。
 *
 * @param formId         表单稳定 ID
 * @param fieldId        字段稳定 ID
 * @param tenantIdentity 租户或已解析路由的稳定隔离标识；公共表可传空字符串
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public record ProtectedFieldContext(String formId, String fieldId, String tenantIdentity) {

    private static final int MAX_COMPONENT_BYTES = 512;

    /** 完成上下文规范化和有界校验。 */
    public ProtectedFieldContext {
        formId = requireText(formId, "protected form id");
        fieldId = requireText(fieldId, "protected field id");
        // 租户身份来自类型化 codec 编码，任何再规范化都会把两个不同租户折叠到同一密码学上下文。
        tenantIdentity = tenantIdentity == null ? "" : tenantIdentity;
        requireBounded(formId);
        requireBounded(fieldId);
        requireBounded(tenantIdentity);
    }

    byte[] aad() {
        return encode("encryption");
    }

    byte[] derivationInfo(String purpose) {
        return encode(requireText(purpose, "protected key purpose"));
    }

    private byte[] encode(String purpose) {
        byte[] purposeBytes = purpose.getBytes(StandardCharsets.UTF_8);
        byte[] formBytes = formId.getBytes(StandardCharsets.UTF_8);
        byte[] fieldBytes = fieldId.getBytes(StandardCharsets.UTF_8);
        byte[] tenantBytes = tenantIdentity.getBytes(StandardCharsets.UTF_8);
        ByteBuffer target = ByteBuffer.allocate(20 + purposeBytes.length + formBytes.length
                                                        + fieldBytes.length + tenantBytes.length);
        target.putInt(ProtectedFieldEnvelope.MAGIC);
        put(target, purposeBytes);
        put(target, formBytes);
        put(target, fieldBytes);
        put(target, tenantBytes);
        return target.array();
    }

    private static void put(ByteBuffer target, byte[] value) {
        target.putInt(value.length).put(value);
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    private static void requireBounded(String value) {
        if (value.getBytes(StandardCharsets.UTF_8).length > MAX_COMPONENT_BYTES) {
            throw new IllegalArgumentException("protected field context is too long");
        }
    }
}
