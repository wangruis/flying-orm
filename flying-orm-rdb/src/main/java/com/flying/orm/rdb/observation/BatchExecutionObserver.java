package com.flying.orm.rdb.observation;

import java.util.Objects;

/**
 * 批量执行观察者。它看的是分片、汇总和 UNKNOWN 恢复，不看每一行参数。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
@FunctionalInterface
public interface BatchExecutionObserver {

    void onExecution(BatchExecutionObservation observation);

    /**
     * 带本次实际事务来源的批量事件。旧 observer 不必修改，需要区分外部参与和 ORM 自管事务时再覆盖。
     */
    default void onExecution(BatchExecutionObservation observation, SqlTransactionSource transactionSource) {
        onExecution(observation);
    }

    static BatchExecutionObserver noop() {
        return ignored -> {
        };
    }

    /**
     * 把两个批量 observer 合成一个，并隔离每个 observer 自己的普通运行时故障。
     *
     * <p>日志、指标和业务审计可以同时接入；普通回调故障不会阻止另一个回调，也不会反向改变已经确定的
     * 数据库结果，异常图中的 JVM 致命错误则原样传播。组合只发生在客户端启动阶段，
     * 批量热路径里不会重复创建包装对象。</p>
     */
    static BatchExecutionObserver composite(BatchExecutionObserver first, BatchExecutionObserver second) {
        BatchExecutionObserver safeFirst = Objects.requireNonNull(first, "first batch observer must not be null");
        BatchExecutionObserver safeSecond = Objects.requireNonNull(second, "second batch observer must not be null");
        return new BatchExecutionObserver() {
            @Override
            public void onExecution(BatchExecutionObservation observation) {
                BatchExecutionObserver.notify(safeFirst, observation);
                BatchExecutionObserver.notify(safeSecond, observation);
            }

            @Override
            public void onExecution(BatchExecutionObservation observation, SqlTransactionSource transactionSource) {
                BatchExecutionObserver.notify(safeFirst, observation, transactionSource);
                BatchExecutionObserver.notify(safeSecond, observation, transactionSource);
            }
        };
    }

    private static void notify(BatchExecutionObserver observer, BatchExecutionObservation observation) {
        try {
            observer.onExecution(observation);
        } catch (RuntimeException failure) {
            ObservationFailureSupport.rethrowVirtualMachineError(failure);
            // observer 是旁路能力，失败时丢掉本次观测，不能改写批量执行结果。
        }
    }

    private static void notify(BatchExecutionObserver observer,
                               BatchExecutionObservation observation,
                               SqlTransactionSource transactionSource) {
        try {
            observer.onExecution(observation, transactionSource);
        } catch (RuntimeException failure) {
            ObservationFailureSupport.rethrowVirtualMachineError(failure);
            // 事务来源只是旁路观测信息，observer 失败不能改变批量结果。
        }
    }
}
