package com.flying.orm.core.page;

import java.util.Locale;

/**
 * PageSort 描述分页查询中的单个排序项，只保留字段和方向，不直接拼接 SQL。
 *
 * @param field     排序字段
 * @param direction 排序方向
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
public record PageSort(String field, Direction direction) {

    /**
     * 创建排序项并完成基础校验。
     *
     * @param field     排序字段
     * @param direction 排序方向
     */
    public PageSort {
        field = PageNames.requireText(field, "page sort field");
        direction = direction == null ? Direction.ASC : direction;
    }

    /**
     * 创建升序排序项。
     *
     * @param field 排序字段
     * @return 升序排序项
     */
    public static PageSort asc(String field) {
        return new PageSort(field, Direction.ASC);
    }

    /**
     * 创建降序排序项。
     *
     * @param field 排序字段
     * @return 降序排序项
     */
    public static PageSort desc(String field) {
        return new PageSort(field, Direction.DESC);
    }

    /**
     * 返回可用于 SQL 渲染的排序方向关键字。
     *
     * @return SQL 排序方向
     */
    public String sqlKeyword() {
        return direction.name().toLowerCase(Locale.ROOT);
    }

    /**
     * 排序方向。
     *
     * @author wangr
     * @date 2026-07-22
     * @version v1.0
     */
    public enum Direction {
        /**
         * 升序。
         */
        ASC,

        /**
         * 降序。
         */
        DESC
    }
}
