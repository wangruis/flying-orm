package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlRequest;

import java.util.Objects;

/** 一条 work SQL 的执行结果。参数值不复制到日志模型之外，request 仍保持原有只读参数集合。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SqlExecutionStepResult(int stepIndex,
                                     SqlRequest request,
                                     long rowsUpdated,
                                     long durationNanos) {
    public SqlExecutionStepResult {
        request = Objects.requireNonNull(request, "step SQL request must not be null");
        if (stepIndex < 0 || rowsUpdated < 0 || durationNanos < 0) {
            throw new IllegalArgumentException("step index, rows and duration must not be negative");
        }
    }
}
