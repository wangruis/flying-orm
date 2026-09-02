package com.flying.orm.core.field;

import com.flying.orm.core.internal.Names;

import java.util.Locale;

/**
 * 动态字段在 ORM 内核中的稳定身份。
 *
 * <p>{@link #name()} 保留去除首尾空白后的声明名称，供方言渲染；{@link #key()} 是只计算一次的
 * 大小写无关查找键，供条件、Scope、映射和重复检查共享。该类型不处理 SQL 引号，引用规则仍由方言负责。</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v1.0
 */
public final class FieldIdentity {

    private final String name;
    private final String key;

    private FieldIdentity(String name) {
        this.name = name;
        this.key = name.toLowerCase(Locale.ROOT);
    }

    /**
     * 创建字段身份。
     *
     * @param name 字段声明名称
     * @return 保留声明名称且带稳定查找键的身份
     */
    public static FieldIdentity of(String name) {
        return new FieldIdentity(Names.requireText(name, "field name"));
    }

    /** @return 去除首尾空白后的字段声明名称 */
    public String name() {
        return name;
    }

    /** @return 使用 {@link Locale#ROOT} 计算且可安全作为 Map 键的规范名称 */
    public String key() {
        return key;
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof FieldIdentity identity && key.equals(identity.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return name;
    }
}
