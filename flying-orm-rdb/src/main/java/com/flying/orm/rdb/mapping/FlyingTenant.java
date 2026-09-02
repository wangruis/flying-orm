package com.flying.orm.rdb.mapping;

import com.flying.orm.core.form.TenantStrategy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记实体的租户隔离字段，使自动生成的 DynamicForm 默认 fail-closed。
 *
 * <p>标在字段上时直接使用该字段；标在类上时必须通过 {@link #field()} 指定 Java 属性名或物理
 * 列名。默认 {@link TenantStrategy#AUTO} 要求上层提供可信 TenantScope，并自动补入读写条件和值。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
public @interface FlyingTenant {

    /** @return 类级注解指定的 Java 属性名或物理列名 */
    String field() default "";

    /** @return 租户值自动补入或显式核对策略 */
    TenantStrategy strategy() default TenantStrategy.AUTO;
}
