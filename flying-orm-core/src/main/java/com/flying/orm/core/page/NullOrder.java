package com.flying.orm.core.page;

/**
 * 空值在排序结果中的固定位置。
 *
 * <p>keyset 必须把空值顺序写进公开请求，不能依赖数据库各自不同的默认值，否则同一游标换方言后
 * 可能漏行或重复。</p>
 *
 * @author wangr
 * @version v3.2
 */
public enum NullOrder {
    FIRST,
    LAST
}
