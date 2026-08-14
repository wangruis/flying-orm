package com.flying.orm.rdb.id;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证雪花生成器的显式节点边界和并发唯一性。 */
class SnowflakeIdGeneratorTest {

    @Test
    void requiresAValidNodeAndGeneratesUniqueIdsConcurrently() {
        assertThrows(IllegalArgumentException.class, () -> SnowflakeIdGenerator.create(-1));
        assertThrows(IllegalArgumentException.class, () -> SnowflakeIdGenerator.create(1_024));

        SnowflakeIdGenerator generator = SnowflakeIdGenerator.create(17);
        Set<Long> ids = ConcurrentHashMap.newKeySet();
        IntStream.range(0, 2_000).parallel().forEach(index ->
                ids.add((Long) generator.generate(Object.class, "id", Long.class)));

        assertEquals(2_000, ids.size());
    }
}
