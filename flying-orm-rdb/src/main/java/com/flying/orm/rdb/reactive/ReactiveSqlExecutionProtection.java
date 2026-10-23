package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.observation.SqlStatementType;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.function.ToLongFunction;

/**
 * 统一 SQL 执行结果的保护规则。
 * 负责行数和结果集内存上限；执行时限由上层拥有。
 * 这样无论是 R2DBC 执行会话还是接口默认逻辑，表现都一致，便于排查和维护。
 */
final class ReactiveSqlExecutionProtection {

    private ReactiveSqlExecutionProtection() {
    }

    /**
     * 将可恢复的 R2DBC 失败转换为稳定错误分类；JVM 致命错误不能作为 Reactive Streams 的普通错误信号继续包装。
     */
    static Throwable translate(Throwable error) {
        Throwable safeError = Objects.requireNonNull(error, "r2dbc execution error must not be null");
        VirtualMachineError fatal = findVirtualMachineError(safeError);
        return fatal == null ? RdbExceptionTranslator.translate(safeError) : fatal;
    }

    static <T> Flux<T> protectRows(Flux<T> source,
                                   String sql,
                                   SqlExecutionOptions options,
                                   ToLongFunction<T> rowSizer) {
        SqlExecutionOptions safeOptions = options;
        requireSql(sql);
        if (safeOptions.maxRows() <= 0 && safeOptions.maxResultBytes() <= 0) {
            return source;
        }

        Flux<T> protectedSource = Flux.defer(() -> {
            ResultBudget budget = new ResultBudget();
            return source.index()
                         .handle((tuple, sink) -> {
                             long rowIndex = tuple.getT1();
                             if (safeOptions.maxRows() > 0 && rowIndex >= safeOptions.maxRows()) {
                                 sink.error(new SqlRowLimitExceededException(
                                         SqlStatementType.fromSql(sql),
                                         safeOptions.maxRows(),
                                         rowIndex));
                                 return;
                             }

                             if (safeOptions.maxResultBytes() > 0 && rowSizer != null) {
                                 long itemBytes = Math.max(0L, rowSizer.applyAsLong(tuple.getT2()));
                                 long attemptedBytes = saturatingAdd(budget.resultBytes, itemBytes);
                                 // Long.MAX_VALUE 表示估算值或累计值溢出，不能作为精确等值放行。
                                 if (attemptedBytes == Long.MAX_VALUE
                                         || attemptedBytes > safeOptions.maxResultBytes()) {
                                     sink.error(new SqlResultMemoryLimitExceededException(
                                             SqlStatementType.fromSql(sql),
                                             safeOptions.maxResultBytes(),
                                             attemptedBytes,
                                             rowIndex));
                                     return;
                                 }
                                 budget.resultBytes = attemptedBytes;
                             }
                             sink.next(tuple.getT2());
                         });
        });
        return protectedSource;
    }

    private static void requireSql(String sql) {
        Objects.requireNonNull(sql, "sql must not be null");
    }

    private static long saturatingAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    /** Per-subscription state; Reactive Streams serializes signals delivered to {@code handle}. */
    private static final class ResultBudget {
        private long resultBytes;
    }
}
