package com.flying.orm.rdb.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标出逻辑删除字段。
 *
 * <p>标在字段上最省事；标在类上时用 field 指明字段名。值用字符串写，解析时会按字段类型转成数字、布尔或字符串。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
public @interface FlyingLogicDelete {

    /**
     * 标在类上时用它指定逻辑删除字段。标在字段上可以不填。
     */
    String field() default "";

    /**
     * 未删除的值，比如 0、false、N。
     */
    String notDeletedValue() default "0";

    /**
     * 已删除的值，比如 1、true、Y。
     */
    String deletedValue() default "1";
}
