package com.flying.orm.rdb.jdbc;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** 验证极大但合法的批量超时不会在换算纳秒时溢出。 */
class JdbcBatchDeadlineTest {

    @Test
    void acceptsAValidDurationWhoseNanosecondsDoNotFitInLong() {
        assertDoesNotThrow(() -> JdbcBatchSupport.BatchDeadline.start(Duration.ofSeconds(Long.MAX_VALUE))
                                                          .remaining());
    }
}
