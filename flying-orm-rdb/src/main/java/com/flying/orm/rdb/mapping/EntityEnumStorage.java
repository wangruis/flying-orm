package com.flying.orm.rdb.mapping;

/**
 * 说明实体枚举在数据库里怎样保存。
 *
 * <p>{@link #NAME} 保存枚举名字，字段改顺序不会影响旧数据；{@link #ORDINAL} 保存序号，空间更小，
 * 但枚举常量一旦换顺序，旧数据的含义也会跟着变。{@link #NONE} 表示不使用这两种转换，例如普通字段或
 * 由 {@code @EnumValue} 指定持久值的枚举；未声明持久值的枚举默认使用 {@link #NAME}。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum EntityEnumStorage {
    NONE,
    NAME,
    ORDINAL
}
