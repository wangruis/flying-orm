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

    /**
     * 只有需要参数日志的 observer 才覆盖这个开关。执行层据此决定是否把已有参数列表交给 observer，
     * 默认路径不会创建参数快照，也不会遍历参数。
     */
    default boolean requiresParameterValues() {
        return false;
    }

    /**
     * 只有确实要记录事务来源的 observer 才打开这个开关。普通指标通常不需要这项信息，保持关闭可以避免
     * 每条 SQL 都额外解析一次响应式事务上下文。
     */
    default boolean requiresTransactionSource() {
        return false;
    }

    /**
     * 带参数详情的同一条观测回调。默认回退到原来的结构化事件，旧 observer 不需要改代码。
     */
    default void onExecution(SqlExecutionObservation observation, List<Object> parameters) {
        onExecution(observation);
    }

    /**
     * 带本次实际事务来源的结构化回调。旧 observer 继续收到原事件，需要区分事务来源时再覆盖此方法。
     */
    default void onExecution(SqlExecutionObservation observation, SqlTransactionSource transactionSource) {
        onExecution(observation);
    }

    /** 带参数和事务来源的完整回调，参数列表仍然只在 observer 明确需要时传入。 */
    default void onExecution(SqlExecutionObservation observation,
                             List<Object> parameters,
                             SqlTransactionSource transactionSource) {
        onExecution(observation, parameters);
    }

    /**
     * 接收不应覆盖已确定数据库结果的资源清理故障。默认空实现保持普通 Lambda observer 简洁，
     * 需要监控连接池健康时可使用匿名类同时接收两类事件。
     *
     * @param observation 资源清理故障事实
     */
    default void onResourceCleanup(ResourceCleanupObservation observation) {
        // 普通 SQL 指标接入不必强制处理资源事件。
    }

    static SqlExecutionObserver noop() {
        return ignored -> {
        };
    }
}
