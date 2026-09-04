package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 声明实体对应表的主键。
 *
 * <p>复合主键按 {@link #properties()} 中的属性顺序保存，不会重新排序。</p>
 *
 * @author wangr
 * @version v3.2
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TablePrimaryKey {

    /**
     * 数据库中的主键名称；留空表示交给方言或命名规则生成。
     */
    String name() default "";

    /**
     * 组成主键的实体属性，数组顺序就是主键列顺序。
     */
    String[] properties() default {};
}
