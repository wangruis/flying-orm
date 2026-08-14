package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记实体的主键字段，并声明主键值由谁生成。
 *
 * <p>默认 {@link IdType#NONE}，表示框架不替调用方生成主键。只有明确声明了生成策略，
 * ORM 才会在插入前或插入时介入，这样不会因为一个普通的 {@code id} 字段被误当成自增字段。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v2.0.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface TableId {

    /**
     * 数据库主键列名；留空表示使用字段名经过默认命名规则转换后的列名。
     */
    String value() default "";

    /**
     * 主键生成策略。
     */
    IdType type() default IdType.NONE;
}
