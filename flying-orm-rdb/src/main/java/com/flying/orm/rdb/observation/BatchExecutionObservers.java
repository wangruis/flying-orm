package com.flying.orm.rdb.observation;

import java.util.Objects;

/** 批量 observer 的包内安全分发实现，外部实现不能声明自己已经隔离。 */
final class BatchExecutionObservers {

    private BatchExecutionObservers() {
    }

    static BatchExecutionObserver safe(BatchExecutionObserver observer) {
        BatchExecutionObserver safeObserver = Objects.requireNonNull(
                observer, "batch execution observer must not be null");
        if (safeObserver instanceof Isolated) {
            return safeObserver;
        }
        try {
            return safeObserver.enabled() ? new Safe(safeObserver) : safeObserver;
        } catch (RuntimeException failure) {
            return BatchExecutionObserver.noop();
        }
    }

    static BatchExecutionObserver composite(BatchExecutionObserver first,
                                             BatchExecutionObserver second) {
        BatchExecutionObserver safeFirst = safe(Objects.requireNonNull(
                first, "first batch observer must not be null"));
        BatchExecutionObserver safeSecond = safe(Objects.requireNonNull(
                second, "second batch observer must not be null"));
        if (!safeFirst.enabled()) {
            return safeSecond;
        }
        if (!safeSecond.enabled()) {
            return safeFirst;
        }
        return new Composite(safeFirst, safeSecond);
    }

    interface Isolated {
    }

    private record Safe(BatchExecutionObserver delegate)
            implements BatchExecutionObserver, Isolated {

        @Override
        public void onExecution(BatchExecutionObservation observation) {
            try {
                delegate.onExecution(observation);
            } catch (RuntimeException ignored) {
                // 旁路 observer 的普通故障只丢本次事件；直接 Error 原样传播。
            }
        }

        @Override
        public void onExecution(BatchExecutionObservation observation,
                                SqlTransactionSource transactionSource) {
            try {
                delegate.onExecution(observation, transactionSource);
            } catch (RuntimeException ignored) {
                // 同一安全分发边界覆盖带事务来源的回调。
            }
        }
    }

    private record Composite(BatchExecutionObserver first,
                             BatchExecutionObserver second)
            implements BatchExecutionObserver, Isolated {

        @Override
        public void onExecution(BatchExecutionObservation observation) {
            first.onExecution(observation);
            second.onExecution(observation);
        }

        @Override
        public void onExecution(BatchExecutionObservation observation,
                                SqlTransactionSource transactionSource) {
            first.onExecution(observation, transactionSource);
            second.onExecution(observation, transactionSource);
        }
    }
}
