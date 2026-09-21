package com.flying.orm.core.page;

/** 游标排序方向。游标条件和 order by 必须使用同一个方向。 */
/**
 * 游标排序方向；游标条件与 {@code order by} 必须采用同一方向。
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum CursorDirection {
    ASC,
    DESC
}
