package com.flying.orm.rdb.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 验证缓存监控计数达到 long 上限后保持稳定。 */
class OrmCacheSnapshotTest {

    @Test
    void requestCountSaturatesInsteadOfOverflowing() {
        OrmCacheSnapshot snapshot = new OrmCacheSnapshot(
                0, 0, 0, Long.MAX_VALUE, 1, 1D, 0, 0, 0, 0, 0, 0);

        assertEquals(Long.MAX_VALUE, snapshot.requestCount());
    }
}
