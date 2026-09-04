package com.flying.orm.core.page;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * 支持复合排序、混合方向和 nullable 位置的 keyset 请求。
 *
 * <p>关系型模块会依据真实主键或已认证唯一约束补齐稳定 tie-breaker，所以这里不按显式 sort 数量
 * 提前限制 position 数量。它也绝不回退成 offset 分页。</p>
 *
 * @param size 本页最多返回的业务行数
 * @param sorts 调用方声明的稳定排序
 * @param position 第一页为空，后续页为上页发布的位置
 * @author wangr
 * @version v3.2
 */
public record KeysetPageQuery(int size,
                              List<KeysetSort> sorts,
                              CursorPosition position) {

    public KeysetPageQuery {
        if (size < 1 || size >= PageQuery.MAX_SIZE) {
            throw new IllegalArgumentException(
                    "keyset page size must be between 1 and " + (PageQuery.MAX_SIZE - 1));
        }
        sorts = List.copyOf(Objects.requireNonNull(sorts, "keyset sorts must not be null"));
        if (sorts.isEmpty()) {
            throw new IllegalArgumentException("keyset pagination requires at least one sort field");
        }
        sorts.forEach(sort -> Objects.requireNonNull(sort, "keyset sort must not be null"));
        position = Objects.requireNonNull(position, "keyset cursor position must not be null");
    }

    public static KeysetPageQuery first(int size, KeysetSort... sorts) {
        return new KeysetPageQuery(size, copySorts(sorts), CursorPosition.first());
    }

    public static KeysetPageQuery after(int size,
                                        CursorPosition position,
                                        KeysetSort... sorts) {
        CursorPosition safePosition = Objects.requireNonNull(
                position, "keyset cursor position must not be null");
        if (safePosition.isFirst()) {
            throw new IllegalArgumentException("keyset after position must not be empty");
        }
        return new KeysetPageQuery(size, copySorts(sorts), safePosition);
    }

    public boolean firstPage() {
        return position.isFirst();
    }

    private static List<KeysetSort> copySorts(KeysetSort[] sorts) {
        return Arrays.asList(Objects.requireNonNull(sorts, "keyset sorts must not be null"));
    }
}
