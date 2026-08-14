package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 确认迁移监控自身失败时不会改变已经确定的 DDL 执行结果。 */
class SchemaMigrationObserversTest {

    @Test
    void predicateFailureDoesNotEscapeObservation() {
        SchemaMigrationObserver observer = SchemaMigrationObservers.when(ignored -> {
            throw new IllegalStateException("filter failed");
        }, ignored -> {
            throw new AssertionError("observer must not run after its predicate failed");
        });

        assertDoesNotThrow(() -> observer.onMigration(success()));
    }

    /** 普通观测故障仍隔离，但其异常图中的 JVM 致命错误不能被旁路静默吞掉。 */
    @Test
    void propagatesVirtualMachineErrorNestedInObserverFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("schema observer fatal");
        SchemaMigrationObserver observer = SchemaMigrationObservers.safe(ignored -> {
            throw new IllegalStateException("observer wrapper", fatal);
        });

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class, () -> observer.onMigration(success()));

        assertSame(fatal, observed);
    }

    /** 迁移 observer 深层包装的 VME 仍须保持原对象，不能被观测隔离吞掉。 */
    @Test
    void propagatesDeeplyNestedVirtualMachineErrorFromObserver() {
        OutOfMemoryError fatal = new OutOfMemoryError("deep schema observer fatal");
        RuntimeException wrapper = new IllegalStateException("wrapper-0", fatal);
        for (int depth = 1; depth < 70; depth++) {
            wrapper = new IllegalStateException("wrapper-" + depth, wrapper);
        }
        RuntimeException failure = wrapper;
        SchemaMigrationObserver observer = SchemaMigrationObservers.safe(ignored -> { throw failure; });

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class, () -> observer.onMigration(success()));

        assertSame(fatal, observed);
    }

    @Test
    void slowObserverKeepsDurationBeyondNanosecondRangeExact() {
        List<SchemaMigrationObservation> received = new ArrayList<>();
        SchemaMigrationObserver observer = SchemaMigrationObservers.slow(
                Duration.ofSeconds(Long.MAX_VALUE), received::add);

        observer.onMigration(success(Long.MAX_VALUE));

        assertEquals(0, received.size());
    }

    private static SchemaMigrationObservation success() {
        return success(1L);
    }

    private static SchemaMigrationObservation success(long durationNanos) {
        return new SchemaMigrationObservation("plan-1",
                                              SchemaMigrationRiskLevel.LOW,
                                              SqlExecutionStatus.SUCCESS,
                                              1,
                                              1,
                                              1,
                                              durationNanos,
                                              SqlFailureCategory.NONE,
                                              null,
                                              null,
                                              null);
    }
}
