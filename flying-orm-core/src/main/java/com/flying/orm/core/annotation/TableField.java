package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 配置实体字段和数据库列之间的映射及写入规则。
 *
 * <p>默认情况下字段参与查询和写入，列名按字段名推导。需要把一个属性排除在表结构之外时，
 * 使用 {@code exist = false}；查询时不希望返回某列时，使用 {@code select = false}。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v2.0.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface TableField {

    /**
     * 数据库列名；留空表示使用字段名经过默认命名规则转换后的列名。
     */
    String value() default "";

    /**
     * 是否真的对应数据库列；临时计算属性应设为 {@code false}。
     */
    boolean exist() default true;

    /**
     * 是否加入查询投影；默认查询这个字段。
     */
    boolean select() default true;

    /**
     * 自动填充时机；默认不自动填充。
     */
    FieldFill fill() default FieldFill.DEFAULT;

    /**
     * 插入时的字段值策略。
     */
    FieldStrategy insertStrategy() default FieldStrategy.DEFAULT;

    /**
     * 更新时的字段值策略。
     */
    FieldStrategy updateStrategy() default FieldStrategy.DEFAULT;
}
