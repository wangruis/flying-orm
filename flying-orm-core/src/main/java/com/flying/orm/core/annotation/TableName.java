package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 指定实体对应的数据库表名。
 *
 * <p>不写 {@link #value()} 时，后续映射层可以按实体类名推导表名；写了以后就以这里的名字为准。
 * 表名属于结构信息，所以注解只能放在实体类上。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v2.0.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TableName {

    /**
     * 数据库表名；留空表示交给默认命名规则推导。
     */
    String value() default "";

    /**
     * 可选的数据库 schema。留空表示由数据源、连接路由或上层默认 schema 决定，
     * 不会把某个环境的 schema 硬编码进普通实体。
     */
    String schema() default "";
}
