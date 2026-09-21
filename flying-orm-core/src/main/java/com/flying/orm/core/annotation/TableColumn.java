package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 补充实体属性对应列的数据库结构信息。
 *
 * <p>这里只声明不能从 Java 类型可靠推导的结构属性，不承担属性名到列名的映射；
 * 列名仍由现有字段映射规则负责。</p>
 *
 * @author wangr
 * @version v3.2
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.RECORD_COMPONENT})
public @interface TableColumn {

    /**
     * 数据库类型的稳定标识；留空表示按实体类型映射规则推导。
     */
    String databaseTypeId() default "";

    /**
     * 字符或二进制长度；{@code -1} 表示未显式指定。
     */
    int length() default -1;

    /**
     * 数值总位数；{@code -1} 表示未显式指定。
     */
    int precision() default -1;

    /**
     * 数值小数位数；{@code -1} 表示未显式指定。
     */
    int scale() default -1;

    /**
     * 时间类型的小数秒精度；{@code -1} 表示未显式指定。
     */
    int temporalPrecision() default -1;

    /**
     * 列是否允许为空；默认交给编译器结合实体属性推导。
     */
    Nullability nullable() default Nullability.INFER;

    /**
     * 默认值定义的稳定标识；留空表示没有显式默认值。
     */
    String defaultId() default "";

    /**
     * 列注释；留空表示不声明注释。
     */
    String comment() default "";

    /**
     * 列值生成方式；默认交给编译器结合现有主键策略推导。
     */
    Generation generation() default Generation.INFER;

    /**
     * 列字符集；留空表示由数据库或表级配置决定。
     */
    String charset() default "";

    /**
     * 列排序规则；留空表示由数据库或表级配置决定。
     */
    String collation() default "";

    /**
     * 列的可空性声明。
     */
    enum Nullability {
        INFER,
        NULLABLE,
        NOT_NULL
    }

    /**
     * 列值生成方式。
     */
    enum Generation {
        INFER,
        NONE,
        IDENTITY
    }
}
