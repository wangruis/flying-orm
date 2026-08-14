package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 指定实体主键使用的数据库序列名。
 *
 * <p>主要给没有自增列、需要显式序列的数据库使用。留空表示不指定序列，避免空配置被当成真实名称。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v2.0.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface KeySequence {

    /**
     * 数据库序列名。
     */
    String value() default "";
}
