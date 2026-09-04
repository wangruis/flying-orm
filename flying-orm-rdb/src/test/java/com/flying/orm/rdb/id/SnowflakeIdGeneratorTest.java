package com.flying.orm.rdb.id;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SnowflakeIdGeneratorTest {

    @Test
    void rejectsClockBeforeEpochWithoutAdvancingState() throws ReflectiveOperationException {
        SnowflakeIdGenerator generator = SnowflakeIdGenerator.create(7L);
        Field epoch = SnowflakeIdGenerator.class.getDeclaredField("epochMillis");
        epoch.setAccessible(true);
        epoch.setLong(generator, System.currentTimeMillis() + 60_000L);

        assertThrows(IllegalStateException.class,
                     () -> generator.generate(Object.class, "id", Long.class));

        Field state = SnowflakeIdGenerator.class.getDeclaredField("state");
        state.setAccessible(true);
        assertEquals(0L, ((AtomicLong) state.get(generator)).get());
    }
}
