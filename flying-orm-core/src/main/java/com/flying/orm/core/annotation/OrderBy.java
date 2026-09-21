package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 声明实体字段在 Repository 查询中的默认排序。
 *
 * <p>注解只能标在已经映射的字段上，不接收 SQL 片段。调用方显式指定排序时，应以显式排序为准。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v2.0.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface OrderBy {

    /** {@code true} 为升序，{@code false} 为降序。 */
    boolean asc() default true;

    /** 多个默认排序字段的先后顺序，数值越小越靠前。 */
    int sort() default 0;
}
