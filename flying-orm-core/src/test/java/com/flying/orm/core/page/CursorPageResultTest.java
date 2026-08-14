package com.flying.orm.core.page;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证游标分页结果不会把可变排序值泄漏给下一页请求。
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class CursorPageResultTest {

    /** hasMore 表示调用方可以继续请求下一页，因此必须同时提供可用的游标。 */
    @Test
    void rejectsMissingNextCursorWhenMoreRowsExist() {
        assertThrows(IllegalArgumentException.class,
                     () -> new CursorPageResult<>(List.of("row"), List.of(), true));
    }

    /** 自定义 codec 返回的数组游标在结果创建和读取时都必须保持快照。 */
    @Test
    void snapshotsArrayNextCursorValuesAtTheResultBoundary() {
        Object[] customKey = {"tenant", 7};
        CursorPageResult<String> result = new CursorPageResult<>(List.of("row"), List.of((Object) customKey), true);

        customKey[0] = "changed";
        assertArrayEquals(new Object[]{"tenant", 7}, (Object[]) result.nextCursor().getFirst());

        ((Object[]) result.nextCursor().getFirst())[1] = 9;
        assertArrayEquals(new Object[]{"tenant", 7}, (Object[]) result.nextCursor().getFirst());
    }
}
