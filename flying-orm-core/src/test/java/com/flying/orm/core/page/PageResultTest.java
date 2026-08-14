package com.flying.orm.core.page;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证分页结果模型会冻结行数据并提供常用分页派生信息。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
class PageResultTest {

    /**
     * 验证分页结果可以计算总页数和下一页状态。
     */
    @Test
    void createsPageResultWithDerivedPageInformation() {
        PageResult<Map<String, Object>> result = PageResult.of(List.of(Map.of("id", "u1")), 41L, PageQuery.of(2, 20));

        assertEquals(1, result.rows().size());
        assertEquals(41L, result.total());
        assertEquals(2, result.page());
        assertEquals(20, result.size());
        assertEquals(3, result.totalPages());
        assertTrue(result.hasNext());
        assertThrows(UnsupportedOperationException.class, () -> result.rows().add(Map.of("id", "u2")));
    }

    /**
     * 验证尾页和空结果能得到确定性的派生信息。
     */
    @Test
    void handlesLastAndEmptyPageResult() {
        PageResult<Map<String, Object>> lastPage = PageResult.of(List.of(Map.of("id", "u1")), 40L, PageQuery.of(2, 20));
        PageResult<Map<String, Object>> empty = PageResult.of(List.of(), 0L, PageQuery.of(1, 20));

        assertFalse(lastPage.hasNext());
        assertEquals(2, lastPage.totalPages());
        assertFalse(empty.hasNext());
        assertEquals(0, empty.totalPages());
    }

    @Test
    void calculatesTotalPagesWithoutOverflowingLong() {
        PageResult<Object> result = new PageResult<>(List.of(), Long.MAX_VALUE, 1, 1000);

        assertEquals(1 + (Long.MAX_VALUE - 1) / 1000, result.totalPages());
        assertTrue(result.hasNext());
    }
}
