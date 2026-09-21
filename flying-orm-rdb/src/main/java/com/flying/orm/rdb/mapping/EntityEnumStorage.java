package com.flying.orm.rdb.mapping;

/**
 * 说明实体枚举在数据库里怎样保存。
 *
 * <p>{@link #NAME} 保存枚举名字，字段改顺序不会影响旧数据；{@link #ORDINAL} 保存序号，空间更小，
 * 但枚举常量一旦换顺序，旧数据的含义也会跟着变。{@link #NONE} 表示字段不是枚举，或没有显式声明
 * JPA {@code @Enumerated}，继续使用 flying-orm 原有的名字存储约定。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum EntityEnumStorage {
    NONE,
    NAME,
    ORDINAL
}
