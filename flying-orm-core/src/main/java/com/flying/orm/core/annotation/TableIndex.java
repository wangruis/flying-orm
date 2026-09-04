package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 声明实体对应表的一项索引。
 *
 * <p>{@link #id()} 提供稳定模型身份；索引列及其方向按声明顺序保存。</p>
 *
 * @author wangr
 * @version v3.2
 */
@Inherited
@Repeatable(TableIndex.List.class)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TableIndex {

    /**
     * 索引在元数据模型中的稳定标识。
     */
    String id();

    /**
     * 数据库中的索引名称；留空表示交给方言或命名规则生成。
     */
    String name() default "";

    /**
     * 是否创建唯一索引。
     */
    boolean unique() default false;

    /**
     * 索引列，数组顺序就是数据库索引键顺序。
     */
    TableIndexColumn[] columns();

    /**
     * Java 编译器保存多项 {@link TableIndex} 声明时使用的容器。
     */
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @interface List {

        /**
         * 同一个实体上的全部索引。
         */
        TableIndex[] value();
    }
}
