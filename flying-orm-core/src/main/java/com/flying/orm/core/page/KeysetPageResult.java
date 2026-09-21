package com.flying.orm.core.page;

import java.util.List;
import java.util.Objects;

/**
 * keyset 分页结果。
 *
 * <p>nextPosition 可以包含 planner 自动加入且已从业务行剥离的 tie-breaker。它当前携带可直接读取的
 * 原始排序值，并不是不透明令牌；启用字段治理时，所有最终排序字段都必须允许 FULL 发布。需要隐藏这些值的
 * 上层应在传输边界把完整位置签名或加密，并在下一次请求前还原。</p>
 *
 * @param rows 本页业务行
 * @param nextPosition 有下一页时使用的完整类型化位置
 * @param hasMore 是否还有下一页
 * @param <T> 业务行类型
 * @author wangr
 * @version v3.2
 */
public record KeysetPageResult<T>(List<T> rows,
                                  CursorPosition nextPosition,
                                  boolean hasMore) {

    public KeysetPageResult {
        rows = List.copyOf(Objects.requireNonNull(rows, "keyset page rows must not be null"));
        nextPosition = Objects.requireNonNull(
                nextPosition, "keyset next position must not be null");
        if (hasMore && nextPosition.isFirst()) {
            throw new IllegalArgumentException(
                    "keyset next position must not be empty when the page has more rows");
        }
    }
}
