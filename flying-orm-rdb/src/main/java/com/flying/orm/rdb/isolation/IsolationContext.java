package com.flying.orm.rdb.isolation;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 一次数据库调用使用的隔离信息。
 *
 * <p>它只描述调用方已经确认的隔离结果，不负责识别当前用户，也不读取 ThreadLocal。数据库键用于选择独立
 * ConnectionFactory；schema 用于同库分 schema；RLS 设置交给数据库会话定制器。三者可以单独使用，也可以组合。</p>
 *
 * @param tenantId 上层确认过的租户标识，只用于审计和路由诊断
 * @param databaseKey 独立数据库连接工厂的稳定键；为空时使用默认连接工厂，外部事务中必须与锁定路由一致
 * @param schema schema 名；为空时保持连接默认 schema
 * @param rlsSettings 数据库原生 RLS 会话变量，例如 {@code app.tenant_id -> tenant-7}
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record IsolationContext(String tenantId,
                               String databaseKey,
                               String schema,
                               Map<String, String> rlsSettings) {

    private static final IsolationContext SHARED = new IsolationContext(null, null, null, Map.of());

    public IsolationContext {
        tenantId = optionalText(tenantId);
        databaseKey = optionalText(databaseKey);
        schema = optionalSchema(schema);
        Map<String, String> copied = new LinkedHashMap<>();
        Objects.requireNonNull(rlsSettings, "RLS settings must not be null").forEach((name, value) -> {
            String safeName = requireText(name, "RLS setting name");
            if (!safeName.matches("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)+")) {
                throw new IllegalArgumentException("RLS setting name must use dotted identifiers");
            }
            copied.put(safeName, Objects.requireNonNull(value, "RLS setting value must not be null"));
        });
        rlsSettings = Collections.unmodifiableMap(copied);
    }

    /** @return 不切库、不切 schema、也不设置 RLS 的共享库上下文 */
    public static IsolationContext shared() {
        return SHARED;
    }

    /** 创建独立数据库路由上下文。 */
    public static IsolationContext database(String tenantId, String databaseKey) {
        return new IsolationContext(requireText(tenantId, "tenant id"),
                                    requireText(databaseKey, "database route key"),
                                    null,
                                    Map.of());
    }

    /** 返回带 schema 的新上下文，原对象不变。 */
    public IsolationContext withSchema(String schema) {
        return new IsolationContext(tenantId, databaseKey, schema, rlsSettings);
    }

    /** 返回带 RLS 会话值的新上下文，值会立即复制。 */
    public IsolationContext withRlsSettings(Map<String, String> settings) {
        return new IsolationContext(tenantId, databaseKey, schema, settings);
    }

    private static String optionalText(String value) {
        return value == null ? null : requireText(value, "isolation context value");
    }

    private static String optionalSchema(String value) {
        if (value == null) {
            return null;
        }
        String safeSchema = requireText(value, "schema");
        if (!safeSchema.matches("[A-Za-z_][A-Za-z0-9_$]*")) {
            throw new IllegalArgumentException("schema contains unsafe characters");
        }
        return safeSchema;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }
}
