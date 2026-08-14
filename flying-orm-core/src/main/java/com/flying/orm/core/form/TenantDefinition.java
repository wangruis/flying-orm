package com.flying.orm.core.form;

import java.util.Objects;

/**
 * 动态表单的租户字段和处理方式。
 *
 * <p>它只描述表单需要怎样隔离租户；具体的补值和校验由 rdb 写入链路处理，避免 core 依赖执行技术。</p>
 *
 * @param fieldName 租户字段名
 * @param strategy  租户处理方式
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public record TenantDefinition(String fieldName, TenantStrategy strategy) {

    public TenantDefinition {
        fieldName = FormNames.requireText(fieldName, "tenant field name");
        strategy = Objects.requireNonNull(strategy, "tenant strategy must not be null");
        if (strategy == TenantStrategy.NONE) {
            throw new IllegalArgumentException("tenant definition strategy must be AUTO or MANUAL");
        }
    }

    public static TenantDefinition of(String fieldName, TenantStrategy strategy) {
        return new TenantDefinition(fieldName, strategy);
    }
}
