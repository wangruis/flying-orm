package com.flying.orm.core.internal;

import java.util.Locale;

/**
 * Core 内部文本名称的唯一基础规则。
 *
 * <p>领域类型决定一个名称代表字段、参数还是 term；这里仅集中无歧义的空白校验、
 * 大小写无关键和 HashMap 容量计算，避免各包复制相同实现。</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v1.0
 */
public final class Names {

    private Names() {
    }

    public static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    public static String key(String value, String name) {
        return requireText(value, name).toLowerCase(Locale.ROOT);
    }

    public static int mapCapacity(int size) {
        return Math.max(4, (int) (size / 0.75F) + 1);
    }
}
