package com.flying.orm.core.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 描述索引中的一个实体属性及其排序方向。
 *
 * <p>该注解只作为 {@link TableIndex#columns()} 的组成部分使用，属性顺序由外层数组决定。</p>
 *
 * @author wangr
 * @version v3.2
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface TableIndexColumn {

    /**
     * 参与索引的实体属性名。
     */
    String property();

    /**
     * 索引列的排序方向。
     */
    Direction direction() default Direction.ASC;

    /**
     * 索引列支持的排序方向。
     */
    enum Direction {
        ASC,
        DESC
    }
}
