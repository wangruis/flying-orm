package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 声明实体对应数据库表的注释。
 *
 * <p>表注释只在显式的关系 Schema 编译、审阅和同步冷路径中读取，不会进入普通 CRUD 热路径。
 * 独立注解也避免给既有 {@link TableName} 增加元素，从而保持已有公开注解的 API/ABI 不变。</p>
 *
 * @author wangr
 * @version v3.2
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TableComment {

    /** 数据库表注释；首尾空白会被去除，空白值按未声明处理。 */
    String value();
}
