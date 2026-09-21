package com.flying.orm.rdb.observation;

import java.util.List;

/**
 * SQL 执行观察者。默认不做事，上层想接日志、指标、链路追踪时再传实现。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
@FunctionalInterface
public interface SqlExecutionObserver {

    void onExecution(SqlExecutionObservation observation);

    /** 关闭时执行器直接返回原执行结果，不创建计时、计数或 Publisher 包装。 */
    default boolean enabled() {
        return true;
    }

    /**
     * 只有需要参数日志的 observer 才覆盖这个开关。执行层据此决定是否把已有参数列表交给 observer，
     * 默认路径不会创建参数快照，也不会遍历参数。
     */
    default boolean requiresParameterValues() {
        return false;
    }


    /**
     * 带参数详情的同一条观测回调。默认回退到原来的结构化事件，旧 observer 不需要改代码。
     */
    default void onExecution(SqlExecutionObservation observation, List<Object> parameters) {
        onExecution(observation);
    }



    /**
     * 接收不应覆盖已确定数据库结果的资源清理故障。默认空实现保持普通 Lambda observer 简洁，
     * 需要记录上层释放或语句资源清理故障时可同时接收两类事件。
     *
     * @param observation 资源清理故障事实
     */
    default void onResourceCleanup(ResourceCleanupObservation observation) {
        // 普通 SQL 指标接入不必强制处理资源事件。
    }

    static SqlExecutionObserver noop() {
        return new SqlExecutionObserver() {
            @Override
            public boolean enabled() {
                return false;
            }

            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // 明确关闭观测。
            }
        };
    }
}
