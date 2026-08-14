package com.flying.orm.rdb.migration;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReactiveDataMigrationTest {

    /** 不显式传 options 时要继承执行器默认保护，不能把统一配置改回 unlimited。 */
    @Test
    void keepsExecutorDefaultOptionsWhenMigrationDoesNotOverrideThem() {
        RecordingExecutor executor = new RecordingExecutor();
        SqlExecutionOptions defaults = SqlExecutionOptions.maxRows(1).withTimeout(Duration.ofSeconds(2));
        DataMigrationPlan plan = DataMigrationPlan.builder("protected-migration")
                .step("first", request("update users set status=?", "ready"), request("update users set status=?", "old"))
                .build();

        ReactiveDataMigration.create(executor.withDefaultExecutionOptions(defaults)).execute(plan).block();

        assertEquals(defaults, executor.options);
    }

    @Test
    void compensatesCompletedStepsInReverseOrderAfterFailure() {
        RecordingExecutor executor = new RecordingExecutor();
        DataMigrationPlan plan = DataMigrationPlan.builder("backfill-user-status")
                .step("first", request("update users set status=? where id=?", "ready", 1),
                       request("update users set status=? where id=?", "old", 1))
                .step("second", request("fail second", "ready", 2),
                       request("update users set status=? where id=?", "old", 2))
                .build();

        DataMigrationException failure = ReactiveDataMigration.create(executor)
                .execute(plan)
                .onErrorResume(DataMigrationException.class, error -> Mono.just(error.result()).flatMap(result -> Mono.error(error)))
                .then(Mono.<DataMigrationException>empty())
                .onErrorResume(DataMigrationException.class, Mono::just)
                .block();

        assertEquals(List.of("update users set status=? where id=?", "fail second",
                             "update users set status=? where id=?"), executor.sql);
        assertEquals(DataMigrationStatus.ROLLED_BACK, failure.result().status());
        assertTrue(failure.result().steps().getFirst().rolledBack());
    }

    /** 补偿结果不能回显驱动异常中的 SQL、连接信息或业务值。 */
    @Test
    void sanitizesRollbackFailureInPublicResult() {
        String secret = "password=do-not-return";
        RecordingExecutor executor = new RecordingExecutor();
        DataMigrationPlan plan = DataMigrationPlan.builder("failed-compensation")
                .step("first", request("update first"), request("fail rollback " + secret))
                .step("second", request("fail second"), request("rollback second"))
                .build();

        DataMigrationException failure = assertThrows(DataMigrationException.class,
                () -> ReactiveDataMigration.create(executor).execute(plan).block());

        String rollbackFailure = failure.result().steps().getFirst().rollbackFailure();
        assertEquals(DataMigrationStatus.ROLLBACK_FAILED, failure.result().status());
        assertEquals("data migration rollback failed", rollbackFailure);
        assertFalse(rollbackFailure.contains(secret));
    }

    /** 迁移失败消息保持有界，计划标识只通过结果对象提供结构化定位。 */
    @Test
    void doesNotExposeUnboundedPlanIdInFailureMessage() {
        String planId = "migration-secret-" + "x".repeat(5_000);
        DataMigrationPlan plan = DataMigrationPlan.builder(planId)
                .step("failed", request("fail forward"), request("rollback"))
                .build();

        DataMigrationException failure = assertThrows(DataMigrationException.class,
                () -> ReactiveDataMigration.create(new RecordingExecutor()).execute(plan).block());

        assertEquals(planId, failure.result().planId());
        assertEquals("data migration failed: status=ROLLED_BACK", failure.getMessage());
        assertFalse(failure.getMessage().contains(planId));
    }

    /**
     * 验证补偿 SQL 发出 JVM 致命错误时，execute 公共边界原样传播，而不降级成 ROLLBACK_FAILED 结果。
     */
    @Test
    void propagatesFatalRollbackFailureWithoutCompensationResult() {
        IllegalStateException forwardFailure = new IllegalStateException("forward failed");
        OutOfMemoryError rollbackFatal = new OutOfMemoryError("rollback fatal");
        List<String> sql = new ArrayList<>();
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                sql.add(request.sql());
                return switch (request.sql()) {
                    case "fail forward" -> Mono.error(forwardFailure);
                    case "fatal rollback" -> Mono.error(rollbackFatal);
                    default -> Mono.just(1L);
                };
            }
        };
        DataMigrationPlan plan = DataMigrationPlan.builder("fatal-compensation")
                .step("first", request("forward first"), request("fatal rollback"))
                .step("second", request("fail forward"), request("rollback second"))
                .build();

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> ReactiveDataMigration.create(executor).execute(plan).block());

        assertSame(rollbackFatal, observed);
        assertEquals(List.of("forward first", "fail forward", "fatal rollback"), sql);
    }

    /**
     * 驱动适配层用普通异常包装 JVM 致命错误时，补偿边界仍必须原样传播致命错误，不能降级为可继续处理的补偿结果。
     */
    @Test
    void propagatesNestedFatalRollbackFailureWithoutCompensationResult() {
        IllegalStateException forwardFailure = new IllegalStateException("forward failed");
        OutOfMemoryError rollbackFatal = new OutOfMemoryError("rollback fatal");
        IllegalStateException wrappedFatal = new IllegalStateException("driver wrapper", rollbackFatal);
        List<String> sql = new ArrayList<>();
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                sql.add(request.sql());
                return switch (request.sql()) {
                    case "fail forward" -> Mono.error(forwardFailure);
                    case "wrapped fatal rollback" -> Mono.error(wrappedFatal);
                    default -> Mono.just(1L);
                };
            }
        };
        DataMigrationPlan plan = DataMigrationPlan.builder("wrapped-fatal-compensation")
                .step("first", request("forward first"), request("wrapped fatal rollback"))
                .step("second", request("fail forward"), request("rollback second"))
                .build();

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                () -> ReactiveDataMigration.create(executor).execute(plan).block());

        assertSame(rollbackFatal, observed);
        assertEquals(List.of("forward first", "fail forward", "wrapped fatal rollback"), sql);
    }

    /** 订阅取消也要补偿已经完成的步骤，不能把半迁移状态留在数据库里。 */
    @Test
    void compensatesCompletedStepsWhenSubscriptionIsCancelled() throws InterruptedException {
        CancellingExecutor executor = new CancellingExecutor();
        DataMigrationPlan plan = DataMigrationPlan.builder("cancelled-migration")
                .step("first", request("forward first"), request("rollback first"))
                .step("second", request("wait second"), request("rollback second"))
                .build();

        Disposable subscription = ReactiveDataMigration.create(executor).execute(plan).subscribe();
        assertTrue(executor.secondStarted.await(1, TimeUnit.SECONDS));

        subscription.dispose();

        assertTrue(executor.rollbackDone.await(1, TimeUnit.SECONDS));
        assertEquals(List.of("forward first", "wait second", "rollback first"), executor.sql);
    }

    private static SqlRequest request(String sql, Object... parameters) {
        return new SqlRequest(sql, List.of(parameters));
    }

    private static final class RecordingExecutor implements ReactiveSqlExecutor {
        private final List<String> sql = new ArrayList<>();
        private SqlExecutionOptions options;

        @Override
        public Flux<DynamicRow> query(SqlRequest request) { return Flux.empty(); }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            sql.add(request.sql());
            return request.sql().startsWith("fail") ? Mono.error(new IllegalStateException("boom")) : Mono.just(1L);
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
            this.options = options;
            return rowsUpdated(request);
        }
    }

    private static final class CancellingExecutor implements ReactiveSqlExecutor {
        private final List<String> sql = new ArrayList<>();
        private final CountDownLatch secondStarted = new CountDownLatch(1);
        private final CountDownLatch rollbackDone = new CountDownLatch(1);

        @Override
        public Flux<DynamicRow> query(SqlRequest request) { return Flux.empty(); }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.defer(() -> {
                sql.add(request.sql());
                if (request.sql().equals("wait second")) {
                    secondStarted.countDown();
                    return Mono.never();
                }
                if (request.sql().equals("rollback first")) {
                    rollbackDone.countDown();
                }
                return Mono.just(1L);
            });
        }
    }
}
