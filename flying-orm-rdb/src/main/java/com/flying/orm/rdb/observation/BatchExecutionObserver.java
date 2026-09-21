package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchExecutionEvidence;

/**
 * 批量执行观察者。它报告累计 SQL 执行事实和汇总，不看每一行参数。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
@FunctionalInterface
public interface BatchExecutionObserver {

    void onExecution(BatchExecutionObservation observation);

    /** 关闭时批量执行链不创建观测包装。 */
    default boolean enabled() {
        return true;
    }


    /**
     * 普通批量形成完整或部分执行证据后调用；默认不做任何事。
     */
    default void onExecutionEvidence(BatchExecutionEvidence evidence) {
        // 未配置事实审计时不额外处理结果。
    }

    static BatchExecutionObserver noop() {
        return new BatchExecutionObserver() {
            @Override
            public boolean enabled() {
                return false;
            }

            @Override
            public void onExecution(BatchExecutionObservation observation) {
                // 明确关闭观测。
            }
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
        return BatchExecutionObservers.composite(first, second);
    }
}
