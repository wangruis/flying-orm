package com.flying.orm.rdb.execution;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;

import java.util.List;
import java.util.Objects;

/** 同连接序列的成功结果，只公开真正 work SQL 的明细，不把会话设置计入 DDL 影响行数。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SqlExecutionSequenceResult(List<SqlExecutionStepResult> workSteps) {
    public SqlExecutionSequenceResult {
        workSteps = List.copyOf(Objects.requireNonNull(workSteps, "sequence work results must not be null"));
    }

    public long rowsUpdated() {
        try {
            return workSteps.stream().mapToLong(SqlExecutionStepResult::rowsUpdated).reduce(0L, Math::addExact);
        } catch (ArithmeticException overflow) {
            throw new RdbException(RdbErrorKind.UNKNOWN,
                                   "database execution count exceeds supported range",
                                   null,
                                   null,
                                   overflow);
        }
    }
}
