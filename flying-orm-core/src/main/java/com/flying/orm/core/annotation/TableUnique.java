package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 声明实体对应表的一项唯一约束。
 *
 * <p>{@link #id()} 是模型内稳定身份，数据库约束名可以独立指定；复合列顺序按属性数组原样保留。</p>
 *
 * @author wangr
 * @version v3.2
 */
@Inherited
@Repeatable(TableUnique.List.class)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TableUnique {

    /**
     * 约束在元数据模型中的稳定标识。
     */
    String id();

    /**
     * 数据库中的约束名称；留空表示交给方言或命名规则生成。
     */
    String name() default "";

    /**
     * 参与唯一约束的实体属性，数组顺序就是约束列顺序。
     */
    String[] properties();

    /**
     * Java 编译器保存多项 {@link TableUnique} 声明时使用的容器。
     */
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @interface List {

        /**
         * 同一个实体上的全部唯一约束。
         */
        TableUnique[] value();
    }
}
