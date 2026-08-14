package com.flying.orm.rdb.observation;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.List;
import java.util.Objects;

/**
 * 把已有 SQL/批量 observer 事件写成安全的一行日志。
 *
 * <p>它不是新的执行器，也不会订阅批量参数流。普通 SQL 的参数值只有在配置明确打开时才会进入格式化器，
 * 并且只读取 {@link SqlRequest} 已有的只读参数列表。日志出口自己的异常会被吞掉，不能反向改变数据库结果。</p>
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class SqlExecutionLogObserver implements SqlExecutionObserver, BatchExecutionObserver {

    private final SqlExecutionLogOptions options;

    private final SqlExecutionLogSelection selection;

    private final SqlExecutionLogSink sink;

    private SqlExecutionLogObserver(SqlExecutionLogOptions options,
                                    SqlExecutionLogSelection selection,
                                    SqlExecutionLogSink sink) {
        this.options = Objects.requireNonNull(options, "sql execution log options must not be null");
        this.selection = Objects.requireNonNull(selection, "sql execution log selection must not be null");
        this.sink = Objects.requireNonNull(sink, "sql execution log sink must not be null");
    }

    /** 保留原来的创建方式，默认完整记录已有的 SQL 与批量事件。 */
    public static SqlExecutionLogObserver create(SqlExecutionLogOptions options, SqlExecutionLogSink sink) {
        return create(options, SqlExecutionLogSelection.defaults(), sink);
    }

    /**
     * 使用独立筛选策略创建日志 observer。展示内容仍由 {@code options} 控制，
     * {@code selection} 只负责字段取舍和慢 SQL、批量事件筛选。
     */
    public static SqlExecutionLogObserver create(SqlExecutionLogOptions options,
                                                 SqlExecutionLogSelection selection,
                                                 SqlExecutionLogSink sink) {
        return new SqlExecutionLogObserver(options, selection, sink);
    }

    /** 日志关闭参数时，观测支持层不会为参数详情创建额外事件对象。 */
    @Override
    public boolean requiresParameterValues() {
        return options.includeParameters();
    }

    /** SQL 日志要明确写出自动提交、ORM 自管或外部事务，因此主动申请事务来源。 */
    @Override
    public boolean requiresTransactionSource() {
        return true;
    }

    @Override
    public void onExecution(SqlExecutionObservation observation) {
        onExecution(observation, SqlTransactionSource.AUTO_COMMIT);
    }

    @Override
    public void onExecution(SqlExecutionObservation observation, SqlTransactionSource transactionSource) {
        SqlExecutionObservation safeObservation = Objects.requireNonNull(observation,
                                                                           "sql observation must not be null");
        SqlTransactionSource safeTransactionSource = Objects.requireNonNull(transactionSource,
                                                                              "transaction source must not be null");
        writeSql(levelOf(safeObservation), safeObservation, null, safeTransactionSource);
    }

    @Override
    public void onExecution(SqlExecutionObservation observation, List<Object> parameters) {
        onExecution(observation, parameters, SqlTransactionSource.AUTO_COMMIT);
    }

    @Override
    public void onExecution(SqlExecutionObservation observation,
                            List<Object> parameters,
                            SqlTransactionSource transactionSource) {
        SqlExecutionObservation safeObservation = Objects.requireNonNull(observation,
                                                                           "sql observation must not be null");
        SqlTransactionSource safeTransactionSource = Objects.requireNonNull(transactionSource,
                                                                              "transaction source must not be null");
        List<Object> safeParameters = Objects.requireNonNull(parameters, "sql parameters must not be null");
        writeSql(levelOf(safeObservation), safeObservation, safeParameters, safeTransactionSource);
    }

    @Override
    public void onExecution(BatchExecutionObservation observation) {
        onExecution(observation, SqlTransactionSource.INTERNAL);
    }

    @Override
    public void onExecution(BatchExecutionObservation observation, SqlTransactionSource transactionSource) {
        BatchExecutionObservation safeObservation = Objects.requireNonNull(observation,
                                                                              "batch observation must not be null");
        SqlTransactionSource safeTransactionSource = Objects.requireNonNull(transactionSource,
                                                                              "transaction source must not be null");
        writeBatch(levelOf(safeObservation), safeObservation, safeTransactionSource);
    }

    @Override
    @InternalApi
    public void onResourceCleanup(ResourceCleanupObservation observation) {
        ResourceCleanupObservation safeObservation = Objects.requireNonNull(
                observation, "resource cleanup observation must not be null");
        if (!isEnabled(SqlExecutionLogLevel.WARN)) {
            return;
        }
        try {
            write(SqlExecutionLogLevel.WARN, SqlExecutionLogFormatter.resourceCleanup(safeObservation, options));
        } catch (RuntimeException failure) {
            ObservationFailureSupport.rethrowVirtualMachineError(failure);
            // 清理日志只是旁路诊断，格式化失败不能改写已经确认的数据库结果。
        }
    }

    /**
     * 先探测级别，再进入格式化器。这样 DEBUG 关闭时不会读取参数，也不会扫描或脱敏 SQL 文本。
     */
    private void writeSql(SqlExecutionLogLevel level,
                          SqlExecutionObservation observation,
                          List<Object> parameters,
                          SqlTransactionSource transactionSource) {
        if (!isEnabled(level)) {
            return;
        }
        try {
            write(level,
                  SqlExecutionLogFormatter.sql(observation,
                                               options,
                                               selection,
                                               parameters,
                                               transactionSource));
        } catch (RuntimeException failure) {
            ObservationFailureSupport.rethrowVirtualMachineError(failure);
            // 格式化只是旁路日志工作；参数展示或脱敏扩展出错时也不能影响数据库结果。
        }
    }

    /** 批量事件同样先做级别探测，避免关闭的日志级别创建批量日志字符串。 */
    private void writeBatch(SqlExecutionLogLevel level,
                            BatchExecutionObservation observation,
                            SqlTransactionSource transactionSource) {
        if (!isEnabled(level)) {
            return;
        }
        try {
            write(level,
                  SqlExecutionLogFormatter.batch(observation,
                                                 options,
                                                 selection,
                                                 transactionSource));
        } catch (RuntimeException failure) {
            ObservationFailureSupport.rethrowVirtualMachineError(failure);
            // 批量观测格式化失败时丢弃日志，不改变已经确定的批量结果。
        }
    }

    private void write(SqlExecutionLogLevel level, String message) {
        if (message == null) {
            return;
        }
        try {
            sink.write(level, message);
        } catch (RuntimeException failure) {
            ObservationFailureSupport.rethrowVirtualMachineError(failure);
            // 日志系统故障不能改变 SQL 的成功、失败、取消或批量 UNKNOWN 结果。
        }
    }

    private boolean isEnabled(SqlExecutionLogLevel level) {
        try {
            return sink.isEnabled(level);
        } catch (RuntimeException failure) {
            ObservationFailureSupport.rethrowVirtualMachineError(failure);
            // 上层日志探测失败时直接跳过本次日志，不能让旁路故障影响数据库调用。
            return false;
        }
    }

    private SqlExecutionLogLevel levelOf(SqlExecutionObservation observation) {
        if (observation.status() == SqlExecutionStatus.SUCCESS) {
            return selection.isSlow(observation.durationNanos())
                    ? SqlExecutionLogLevel.WARN
                    : SqlExecutionLogLevel.DEBUG;
        }
        if (observation.status() == SqlExecutionStatus.CANCELLED
                || isWarningCategory(observation.failureCategory())) {
            return SqlExecutionLogLevel.WARN;
        }
        return SqlExecutionLogLevel.ERROR;
    }

    private SqlExecutionLogLevel levelOf(BatchExecutionObservation observation) {
        SqlExecutionResultKind resultKind = observation.resultKind();
        if (resultKind == SqlExecutionResultKind.SUCCESS || resultKind == SqlExecutionResultKind.ENLISTED) {
            return selection.isSlow(observation.durationNanos())
                    ? SqlExecutionLogLevel.WARN
                    : SqlExecutionLogLevel.DEBUG;
        }
        if (resultKind == SqlExecutionResultKind.CANCELLED
                || resultKind == SqlExecutionResultKind.TIMEOUT
                || resultKind == SqlExecutionResultKind.CONNECTION
                || resultKind == SqlExecutionResultKind.UNKNOWN) {
            return SqlExecutionLogLevel.WARN;
        }
        return SqlExecutionLogLevel.ERROR;
    }

    private static boolean isWarningCategory(SqlFailureCategory category) {
        return category == SqlFailureCategory.CANCELLED
                || category == SqlFailureCategory.TIMEOUT
                || category == SqlFailureCategory.CONNECTION
                || category == SqlFailureCategory.UNKNOWN;
    }
}
