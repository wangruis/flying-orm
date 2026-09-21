package com.flying.orm.core.page;

import com.flying.orm.core.field.FieldIdentity;

import java.util.Objects;

/**
 * 新 keyset 分页的一项完整排序事实。
 *
 * <p>方向和空值顺序必须同时确定。关系型 planner 会在此基础上补充可靠的唯一 tie-breaker，
 * 这个框架无关值对象不猜测表约束。</p>
 *
 * @param field 规范字段身份
 * @param direction 排序方向
 * @param nullOrder 空值固定位置
 * @author wangr
 * @version v3.2
 */
public record KeysetSort(String field,
                         CursorDirection direction,
                         NullOrder nullOrder) {

    public KeysetSort {
        field = FieldIdentity.of(field).key();
        direction = Objects.requireNonNull(direction, "keyset sort direction must not be null");
        nullOrder = Objects.requireNonNull(nullOrder, "keyset null order must not be null");
    }

    public static KeysetSort asc(String field, NullOrder nullOrder) {
        return new KeysetSort(field, CursorDirection.ASC, nullOrder);
    }

    public static KeysetSort desc(String field, NullOrder nullOrder) {
        return new KeysetSort(field, CursorDirection.DESC, nullOrder);
    }
}
