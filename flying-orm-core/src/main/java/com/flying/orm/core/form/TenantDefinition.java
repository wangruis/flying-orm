package com.flying.orm.core.form;

import com.flying.orm.core.field.FieldIdentity;

import java.util.Objects;

/**
 * 动态表单的租户字段和处理方式。
 *
 * <p>它只描述表单需要怎样隔离租户；具体的补值和校验由 rdb 写入链路处理，避免 core 依赖执行技术。</p>
 *
 * @param identity 租户字段身份
 * @param strategy  租户处理方式
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public record TenantDefinition(FieldIdentity identity, TenantStrategy strategy) {

    public TenantDefinition {
        identity = Objects.requireNonNull(identity, "tenant field identity must not be null");
        strategy = Objects.requireNonNull(strategy, "tenant strategy must not be null");
        if (strategy == TenantStrategy.NONE) {
            throw new IllegalArgumentException("tenant definition strategy must be AUTO or MANUAL");
        }
    }

    public TenantDefinition(String fieldName, TenantStrategy strategy) {
        this(FieldIdentity.of(fieldName), strategy);
    }

    /** @return 保留声明大小写的租户字段名 */
    public String fieldName() {
        return identity.name();
    }

    public static TenantDefinition of(String fieldName, TenantStrategy strategy) {
        return new TenantDefinition(fieldName, strategy);
    }
}
