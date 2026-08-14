package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

/** 验证接口默认保护不会遗漏结果集内存预算。 */
class ReactiveSqlExecutorDefaultOptionsTest {

    @Test
    void defaultOptionsRejectsOversizedCustomExecutorRows() {
        ReactiveSqlExecutor delegate = new FixedRowsExecutor();

        StepVerifier.create(delegate.withDefaultExecutionOptions(
                                    SqlExecutionOptions.safeDefaults().withMaxResultBytes(32))
                                    .query(request()))
                    .expectError(SqlResultMemoryLimitExceededException.class)
                    .verify();
    }

    private static SqlRequest request() {
        return new SqlRequest("select payload from event_log", List.of());
    }

    private static final class FixedRowsExecutor implements ReactiveSqlExecutor {

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.just(DynamicRow.copyOf(Map.of("payload", "01234567890123456789")));
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.just(0L);
        }
    }
}
