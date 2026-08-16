package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * 验证连接已经借到后，单次 R2DBC SQL 执行只使用一个绝对截止时间。
 *
 * @author wangr
 * @date 2026-08-16
 * @version v1.0
 */
class R2dbcSqlDeadlineTest {

    /** SQL 执行超过配置时限时报告稳定的 ORM 执行超时。 */
    @Test
    void expiresMonoExecutionAtConfiguredDeadline() {
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                                                         .withTimeout(Duration.ofSeconds(1));

        StepVerifier.withVirtualTime(() -> R2dbcSqlDeadline.start(options)
                .protectExecution(Mono.never()))
                .thenAwait(Duration.ofSeconds(1))
                .expectError(SqlExecutionTimeoutException.class)
                .verify();
    }

    /** 多行持续到达也不能重置总截止时间。 */
    @Test
    void expiresFluxAtAbsoluteDeadlineDespiteRows() {
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                                                         .withTimeout(Duration.ofSeconds(1));

        StepVerifier.withVirtualTime(() -> R2dbcSqlDeadline.start(options)
                .protectExecution(Flux.interval(Duration.ofMillis(100)).take(20)))
                .thenAwait(Duration.ofMillis(999))
                .expectNextCount(9)
                .thenAwait(Duration.ofMillis(1))
                .expectError(SqlExecutionTimeoutException.class)
                .verify();
    }

    /** 驱动自己的 TimeoutException 不能冒充 ORM 配置截止时间。 */
    @Test
    void preservesSourceTimeoutExceptionDuringExecution() {
        TimeoutException sourceFailure = new TimeoutException("source");

        StepVerifier.create(R2dbcSqlDeadline.start(SqlExecutionOptions.safeDefaults())
                                             .protectExecution(Mono.error(sourceFailure)))
                    .expectErrorSatisfies(error -> org.junit.jupiter.api.Assertions.assertSame(
                            sourceFailure, error))
                    .verify();
    }

    /** 关闭 SQL 兜底时不增加定时保护。 */
    @Test
    void leavesSourceUnwrappedWhenExecutionDeadlineIsDisabled() {
        TimeoutException sourceFailure = new TimeoutException("source");

        StepVerifier.create(R2dbcSqlDeadline.start(SqlExecutionOptions.unlimited())
                                             .protectExecution(Mono.error(sourceFailure)))
                    .expectErrorSatisfies(error -> org.junit.jupiter.api.Assertions.assertSame(
                            sourceFailure, error))
                    .verify();
    }
}
