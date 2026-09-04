package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 用受控操作符声明实体对应表的一项检查约束。
 *
 * <p>约束只引用实体属性和字面值，不接受任意 SQL，具体数据库表达式由方言安全生成。</p>
 *
 * @author wangr
 * @version v3.2
 */
@Inherited
@Repeatable(TableCheck.List.class)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TableCheck {

    /**
     * 检查约束在元数据模型中的稳定标识。
     */
    String id();

    /**
     * 数据库中的约束名称；留空表示交给方言或命名规则生成。
     */
    String name() default "";

    /**
     * 被检查的实体属性名。
     */
    String property();

    /**
     * 检查约束使用的受控比较操作符。
     */
    Operator operator();

    /**
     * 操作符需要的字面值；数组顺序由操作符语义解释。
     */
    String[] literalValues() default {};

    /**
     * 检查约束允许使用的受控操作符。
     */
    enum Operator {
        EQUAL,
        NOT_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        BETWEEN,
        IN,
        IS_NULL,
        IS_NOT_NULL
    }

    /**
     * Java 编译器保存多项 {@link TableCheck} 声明时使用的容器。
     */
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @interface List {

        /**
         * 同一个实体上的全部检查约束。
         */
        TableCheck[] value();
    }
}
