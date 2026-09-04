package com.flying.orm.core.annotation;

import com.flying.orm.core.metadata.ReferentialAction;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 声明实体对应表的一项外键约束。
 *
 * <p>本地属性和目标属性按数组位置一一对应，复合外键的顺序按声明原样保留。</p>
 *
 * @author wangr
 * @version v3.2
 */
@Inherited
@Repeatable(TableForeignKey.List.class)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TableForeignKey {

    /**
     * 外键在元数据模型中的稳定标识。
     */
    String id();

    /**
     * 数据库中的外键名称；留空表示交给方言或命名规则生成。
     */
    String name() default "";

    /**
     * 当前实体中参与外键的属性。
     */
    String[] localProperties();

    /**
     * 外键引用的目标实体类型。
     */
    Class<?> targetEntity();

    /**
     * 目标实体中被引用的属性，与本地属性按位置对应。
     */
    String[] targetProperties();

    /**
     * 目标键更新时采用的引用动作。
     */
    ReferentialAction onUpdate() default ReferentialAction.NO_ACTION;

    /**
     * 目标行删除时采用的引用动作。
     */
    ReferentialAction onDelete() default ReferentialAction.NO_ACTION;

    /**
     * Java 编译器保存多项 {@link TableForeignKey} 声明时使用的容器。
     */
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @interface List {

        /**
         * 同一个实体上的全部外键约束。
         */
        TableForeignKey[] value();
    }
}
