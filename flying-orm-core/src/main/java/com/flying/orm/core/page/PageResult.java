package com.flying.orm.core.page;

import java.util.List;
import java.util.Objects;

/**
 * PageResult 保存分页查询结果和总数信息，行数据集合在创建时冻结。
 *
 * @param rows  当前页行数据
 * @param total 总行数
 * @param page  一基页码
 * @param size  每页数量
 * @param <T>   行数据类型
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
public record PageResult<T>(List<T> rows, long total, int page, int size) {

    /**
     * 创建分页结果。
     *
     * @param rows  当前页行数据
     * @param total 总行数
     * @param page  一基页码
     * @param size  每页数量
     */
    public PageResult {
        rows = List.copyOf(Objects.requireNonNull(rows, "page rows must not be null"));
        if (total < 0) {
            throw new IllegalArgumentException("page total must not be negative");
        }
        if (page < 1) {
            throw new IllegalArgumentException("page must be greater than or equal to 1");
        }
        if (size < 1) {
            throw new IllegalArgumentException("page size must be greater than or equal to 1");
        }
    }

    /**
     * 按分页请求创建分页结果。
     *
     * @param rows  当前页行数据
     * @param total 总行数
     * @param query 分页请求
     * @param <T>   行数据类型
     * @return 分页结果
     */
    public static <T> PageResult<T> of(List<T> rows, long total, PageQuery query) {
        PageQuery safeQuery = Objects.requireNonNull(query, "page query must not be null");
        return new PageResult<>(rows, total, safeQuery.page(), safeQuery.size());
    }

    /**
     * 返回总页数。
     *
     * @return 总页数
     */
    public long totalPages() {
        if (total == 0) {
            return 0;
        }
        // 不能写成 (total + size - 1) / size：total 接近 Long.MAX_VALUE 时加法会溢出成负数。
        // total 已经保证大于 0，先减一再除可以得到同样的向上取整结果，而且整个过程不会越界。
        return 1 + (total - 1) / size;
    }

    /**
     * 返回是否存在下一页。
     *
     * @return 存在下一页时返回 true
     */
    public boolean hasNext() {
        return page < totalPages();
    }
}
