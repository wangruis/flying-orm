package com.flying.orm.core.metadata;

/**
 * 数据库外键在引用行变化时允许采用的受控动作。
 *
 * @author wangr
 * @version v3.2
 */
public enum ReferentialAction {
    NO_ACTION,
    RESTRICT,
    CASCADE,
    SET_NULL,
    SET_DEFAULT
}
