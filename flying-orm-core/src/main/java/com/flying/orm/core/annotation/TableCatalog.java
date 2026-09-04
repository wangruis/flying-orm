package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 指定实体所属的数据库 catalog。
 *
 * <p>catalog 是表身份的一部分，只描述数据库结构，不负责选择数据源或管理连接。</p>
 *
 * @author wangr
 * @version v3.2
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TableCatalog {

    /**
     * catalog 名称。
     */
    String value();
}
