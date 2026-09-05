package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 声明实体表使用受控的分区父表结构。
 *
 * <p>{@link #property()} 必须是一个直接入库的实体属性名，不接受列名或 SQL 表达式。当前只开放
 * 单列时间 {@link Strategy#RANGE}；分区子表的创建、留存和归档仍由上层编排。</p>
 *
 * @author wangr
 * @version v3.3
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TablePartition {

    /** @return 受控分区策略 */
    Strategy strategy();

    /** @return 分区键对应的 Java 实体属性名 */
    String property();

    /** 当前正式支持的分区策略。 */
    enum Strategy {
        RANGE
    }
}
