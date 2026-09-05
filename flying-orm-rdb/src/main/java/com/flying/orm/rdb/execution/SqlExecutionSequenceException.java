package com.flying.orm.rdb.execution;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.util.List;
import java.util.Objects;

/**
 * 同连接序列失败。phase 和 stepIndex 指出失败位置，completedWorkSteps 让上层知道失败前哪些 DDL
 * 已经被数据库接受；DDL 是否回滚仍由数据库和外部事务决定，本异常不会伪造原子性。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class SqlExecutionSequenceException extends RuntimeException implements OrmErrorReportProvider {
    private final SqlExecutionPhase phase;
    private final int stepIndex;
    private final List<SqlExecutionStepResult> completedWorkSteps;

    public SqlExecutionSequenceException(SqlExecutionPhase phase,
                                         int stepIndex,
                                         List<SqlExecutionStepResult> completedWorkSteps,
                                         Throwable cause) {
        super("connection-scoped SQL failed at " + phase + " step " + stepIndex,
              Objects.requireNonNull(cause, "sequence failure cause must not be null"));
        this.phase = Objects.requireNonNull(phase, "sequence failure phase must not be null");
        if (stepIndex < 0) {
            throw new IllegalArgumentException("sequence failure step index must not be negative");
        }
        this.stepIndex = stepIndex;
        this.completedWorkSteps = List.copyOf(Objects.requireNonNull(
                completedWorkSteps, "completed work steps must not be null"));
    }

    /** @return 失败发生在会话准备、真正工作还是会话清理阶段 */
    public SqlExecutionPhase phase() { return phase; }

    /**
     * @return 失败位置，从零开始；两条 SQL 之间超时则指向下一条，阶段已执行完时等于该阶段的语句数
     */
    public int stepIndex() { return stepIndex; }

    /** @return 失败之前已经执行完成的 work SQL，只读且按原执行顺序排列 */
    public List<SqlExecutionStepResult> completedWorkSteps() { return completedWorkSteps; }

    /** @return 明确失败阶段和步骤位置的统一执行报告 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("EXECUTION",
                                  "SEQUENCE_" + phase.name() + "_FAILED",
                                  phase.name(),
                                  "steps[" + stepIndex + "]",
                                  null,
                                  getMessage());
    }
}
