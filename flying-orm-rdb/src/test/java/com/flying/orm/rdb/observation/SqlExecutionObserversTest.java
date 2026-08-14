package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 观测工具本身要足够稳，不能让日志或指标代码影响 SQL 执行链路。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
class SqlExecutionObserversTest {

    @Test
    void compositeKeepsCallingNextObserverWhenOneObserverFails() {
        List<SqlExecutionObservation> received = new ArrayList<>();
        List<SqlTransactionSource> transactionSources = new ArrayList<>();
        SqlExecutionObserver observer = SqlExecutionObservers.composite(
                ignored -> {
                    throw new IllegalStateException("metrics backend failed");
                },
                new SqlExecutionObserver() {
                    @Override
                    public void onExecution(SqlExecutionObservation observation) {
                        received.add(observation);
                    }

                    @Override
                    public void onExecution(SqlExecutionObservation observation,
                                            SqlTransactionSource transactionSource) {
                        received.add(observation);
                        transactionSources.add(transactionSource);
                    }
                });

        observer.onExecution(observation(Duration.ofMillis(10)), SqlTransactionSource.EXTERNAL);

        assertEquals(1, received.size());
        assertEquals(List.of(SqlTransactionSource.EXTERNAL), transactionSources);
    }

    @Test
    void slowObserverOnlyReceivesEventsOverThreshold() {
        List<SqlExecutionObservation> received = new ArrayList<>();
        SqlExecutionObserver observer = SqlExecutionObservers.slow(Duration.ofMillis(100), received::add);

        observer.onExecution(observation(Duration.ofMillis(80)));
        observer.onExecution(observation(Duration.ofMillis(120)));

        assertEquals(1, received.size());
        assertEquals(Duration.ofMillis(120), received.getFirst().duration());
    }

    @Test
    void slowObserverKeepsDurationBeyondNanosecondRangeExact() {
        List<SqlExecutionObservation> received = new ArrayList<>();
        SqlExecutionObserver observer = SqlExecutionObservers.slow(
                Duration.ofSeconds(Long.MAX_VALUE), received::add);

        observer.onExecution(observation(Long.MAX_VALUE));

        assertEquals(0, received.size());
    }

    @Test
    void predicateFailureDoesNotEscapeObservation() {
        SqlExecutionObserver observer = SqlExecutionObservers.when(ignored -> {
            throw new IllegalStateException("filter failed");
        }, ignored -> {
            throw new AssertionError("observer must not run after its predicate failed");
        });

        observer.onExecution(observation(Duration.ofMillis(10)));
    }

    @Test
    void parameterRequirementFailureFallsBackToSafeFastPath() {
        SqlExecutionObserver observer = SqlExecutionObservers.safe(new SqlExecutionObserver() {
            @Override
            public boolean requiresParameterValues() {
                throw new IllegalStateException("log configuration failed");
            }

            @Override
            public void onExecution(SqlExecutionObservation observation) {
            }
        });

        assertFalse(observer.requiresParameterValues());
    }

    @Test
    void sampleEveryUsesStableInterval() {
        List<SqlExecutionObservation> received = new ArrayList<>();
        SqlExecutionObserver observer = SqlExecutionObservers.sampleEvery(2, received::add);

        observer.onExecution(observation(Duration.ofMillis(1)));
        observer.onExecution(observation(Duration.ofMillis(2)));
        observer.onExecution(observation(Duration.ofMillis(3)));
        observer.onExecution(observation(Duration.ofMillis(4)));
        observer.onExecution(observation(Duration.ofMillis(5)));

        assertEquals(2, received.size());
        assertEquals(Duration.ofMillis(2), received.get(0).duration());
        assertEquals(Duration.ofMillis(4), received.get(1).duration());
    }

    @Test
    void rejectsInvalidSamplingArguments() {
        assertThrows(IllegalArgumentException.class, () -> SqlExecutionObservers.sample(-0.1D, ignored -> {
        }));
        assertThrows(IllegalArgumentException.class, () -> SqlExecutionObservers.sample(1.1D, ignored -> {
        }));
        assertThrows(IllegalArgumentException.class, () -> SqlExecutionObservers.sampleEvery(0, ignored -> {
        }));
    }

    @Test
    void classifiesConnectionScopedFailureByItsDatabaseCause() {
        RdbException lockTimeout = new RdbException(
                RdbErrorKind.LOCK_TIMEOUT, "lock timeout", null, 0, new RuntimeException("driver"));
        SqlExecutionSequenceException sequenceFailure = new SqlExecutionSequenceException(
                SqlExecutionPhase.WORK, 1, List.of(), lockTimeout);

        assertEquals(SqlFailureCategory.LOCK_TIMEOUT, SqlFailureCategory.classify(sequenceFailure));
    }

    /** safe observer 只能隔离普通运行时故障，包装的 VME 仍须按原对象传播。 */
    @Test
    void safeObserverPromotesNestedVirtualMachineError() {
        OutOfMemoryError fatal = new OutOfMemoryError("observer nested fatal");
        IllegalStateException wrapper = new IllegalStateException("observer wrapper", fatal);
        SqlExecutionObserver observer = SqlExecutionObservers.safe(ignored -> { throw wrapper; });

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> observer.onExecution(observation(1L)));

        assertSame(fatal, observed);
    }

    /** 批量 observer 组合器也不能把普通包装异常中的 VME 当作可忽略观测故障。 */
    @Test
    void batchObserverCompositePromotesNestedVirtualMachineError() {
        OutOfMemoryError fatal = new OutOfMemoryError("batch observer nested fatal");
        IllegalStateException wrapper = new IllegalStateException("batch observer wrapper", fatal);
        BatchExecutionObserver observer = BatchExecutionObserver.composite(
                ignored -> { throw wrapper; }, BatchExecutionObserver.noop());

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> observer.onExecution(batchObservation()));

        assertSame(fatal, observed);
    }

    private static SqlExecutionObservation observation(Duration duration) {
        return observation(duration.toNanos());
    }

    private static SqlExecutionObservation observation(long durationNanos) {
        return new SqlExecutionObservation(SqlExecutionOperation.QUERY,
                                           SqlStatementType.SELECT,
                                           SqlExecutionStatus.SUCCESS,
                                           SqlFailureCategory.NONE,
                                           "select id from Users",
                                           0,
                                           0,
                                           1L,
                                           durationNanos,
                                           null);
    }

    private static BatchExecutionObservation batchObservation() {
        return BatchExecutionObservation.summary(
                new BatchExecutionObservation.BatchWriteRequestView(
                        "insert into Users(id) values(?)", BatchWriteOptions.Mode.INDEPENDENT, 1),
                BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                                      List.of(BatchChunkResult.committed(0, 0, 1, 1))),
                1L);
    }
}
