package com.flying.orm.core.page;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * PageQuery 描述一基页码分页请求，限制单页最大数量，避免动态表单列表无边界查询。
 *
 * @param page  一基页码
 * @param size  每页数量
 * @param sorts 排序项
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
public record PageQuery(int page, int size, List<PageSort> sorts) {

    /**
     * 单页最大数量。
     */
    public static final int MAX_SIZE = 1000;

    /**
     * 创建分页请求并完成边界校验。
     *
     * @param page  一基页码
     * @param size  每页数量
     * @param sorts 排序项
     */
    public PageQuery {
        if (page < 1) {
            throw new IllegalArgumentException("page must be greater than or equal to 1");
        }
        if (size < 1 || size > MAX_SIZE) {
            throw new IllegalArgumentException("page size must be between 1 and " + MAX_SIZE);
        }
        sorts = List.copyOf(Objects.requireNonNull(sorts, "page sorts must not be null"));
    }

    /**
     * 创建无排序分页请求。
     *
     * @param page 一基页码
     * @param size 每页数量
     * @return 分页请求
     */
    public static PageQuery of(int page, int size) {
        return new PageQuery(page, size, List.of());
    }

    /**
     * 创建带排序分页请求。
     *
     * @param page  一基页码
     * @param size  每页数量
     * @param sorts 排序项数组
     * @return 分页请求
     */
    public static PageQuery of(int page, int size, PageSort... sorts) {
        Objects.requireNonNull(sorts, "page sorts must not be null");
        return new PageQuery(page, size, Arrays.asList(sorts));
    }

    /**
     * 返回数据库 offset。
     *
     * @return offset
     */
    public long offset() {
        return (long) (page - 1) * size;
    }
}
