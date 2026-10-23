package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记实体的逻辑删除字段；类级声明通过 field 指定属性名或列名。
 *
 * <p>默认约定是 {@code 0} 表示未删除、{@code 1} 表示已删除。字符串形式保留在注解中，
 * 让数字、字符串和布尔型字段都能由后续类型转换层按目标列类型处理。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v2.0.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
public @interface TableLogic {

    /** 类级声明所指的 Java 属性名或物理列名；字段级声明无需填写。 */
    String field() default "";

    /**
     * 未删除时写入的值。
     */
    String value() default "0";

    /**
     * 执行逻辑删除时写入的值。
     */
    String delval() default "1";
}
