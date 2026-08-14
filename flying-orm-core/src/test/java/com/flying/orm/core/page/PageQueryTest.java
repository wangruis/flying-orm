package com.flying.orm.core.page;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证分页查询模型的边界校验、offset 计算和排序不可变性。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
class PageQueryTest {

    /**
     * 验证页码使用一基索引，并能稳定计算数据库 offset。
     */
    @Test
    void createsPageQueryWithOffsetAndImmutableSorts() {
        PageQuery query = PageQuery.of(3, 20, PageSort.desc("createdAt"), PageSort.asc("id"));

        assertEquals(3, query.page());
        assertEquals(20, query.size());
        assertEquals(40L, query.offset());
        assertEquals(List.of(PageSort.desc("createdAt"), PageSort.asc("id")), query.sorts());
        assertThrows(UnsupportedOperationException.class, () -> query.sorts().add(PageSort.asc("name")));
    }

    /**
     * 验证非法分页参数会被拒绝，避免无边界列表查询拖垮数据库。
     */
    @Test
    void rejectsInvalidPageQueryArguments() {
        assertThrows(IllegalArgumentException.class, () -> PageQuery.of(0, 20));
        assertThrows(IllegalArgumentException.class, () -> PageQuery.of(1, 0));
        assertThrows(IllegalArgumentException.class, () -> PageQuery.of(1, 1001));
    }
}
