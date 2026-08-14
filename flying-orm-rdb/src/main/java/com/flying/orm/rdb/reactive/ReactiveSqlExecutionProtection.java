package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.observation.SqlStatementType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

/**
 * 统一 SQL 执行结果的保护规则。
 * 负责把「行数上限」、「结果集内存上限」和「超时」这三件事放在同一条链路里做。
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

    /**
     * 在资源域外恢复清理期间封装的虚拟机级错误。
     *
     * <p>异常图可能含有共享节点；这里按对象身份去重，并让每个节点的 suppressed 先于 cause 遍历，
     * 既避免环又保持清理失败优先级。</p>
     */
    static VirtualMachineError findVirtualMachineError(Throwable error) {
        Throwable safeError = Objects.requireNonNull(error, "r2dbc execution error must not be null");
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.addFirst(safeError);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current instanceof VirtualMachineError fatal) {
                return fatal;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addFirst(cause);
            }
            Throwable[] suppressed = current.getSuppressed();
            for (int index = suppressed.length - 1; index >= 0; index--) {
                pending.addFirst(suppressed[index]);
            }
        }
        return null;
    }

    /**
     * 在两个清理阶段错误之间选择应向外传播的 fatal，并把未选中的上下文以无环 suppressed 关系保留。
     */
    static VirtualMachineError promoteVirtualMachineError(Throwable primary, Throwable cleanup) {
        Throwable safePrimary = Objects.requireNonNull(primary, "primary error must not be null");
        Throwable safeCleanup = Objects.requireNonNull(cleanup, "cleanup error must not be null");
        VirtualMachineError primaryFatal = findVirtualMachineError(safePrimary);
        if (primaryFatal != null) {
            addSuppressedIfAcyclic(primaryFatal, safeCleanup);
            return primaryFatal;
        }
        VirtualMachineError cleanupFatal = findVirtualMachineError(safeCleanup);
        if (cleanupFatal != null) {
            addSuppressedIfAcyclic(cleanupFatal, safePrimary);
            return cleanupFatal;
        }
        return null;
    }

    /** 将普通清理上下文挂到主错误上，同时避免 Throwable 图出现环或重复边。 */
    static void addSuppressedIfAcyclic(Throwable primary, Throwable secondary) {
        Throwable safePrimary = Objects.requireNonNull(primary, "primary error must not be null");
        Throwable safeSecondary = Objects.requireNonNull(secondary, "secondary error must not be null");
        if (safePrimary != safeSecondary
                && !reaches(safePrimary, safeSecondary)
                && !reaches(safeSecondary, safePrimary)) {
            safePrimary.addSuppressed(safeSecondary);
        }
    }

    private static boolean reaches(Throwable start, Throwable expected) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.addFirst(start);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == expected) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addFirst(cause);
            }
            for (Throwable suppressed : current.getSuppressed()) {
                pending.addFirst(suppressed);
            }
        }
        return false;
    }

    static <T> Mono<T> protectMono(Mono<T> source, SqlExecutionOptions options) {
        SqlExecutionOptions safeOptions = requireOptions(options);
        if (safeOptions.timeout().isZero()) {
            return source;
        }
        return source.timeout(safeOptions.timeout())
                     .onErrorMap(TimeoutException.class,
                                 error -> new com.flying.orm.rdb.execution.SqlExecutionTimeoutException(
                                         safeOptions.timeout(),
                                         error));
    }

    static <T> Flux<T> protectRows(Flux<T> source, String sql, SqlExecutionOptions options) {
        return protectRows(source, sql, options, null);
    }

    static <T> Flux<T> protectRows(Flux<T> source,
                                   String sql,
                                   SqlExecutionOptions options,
                                   ToLongFunction<T> rowSizer) {
        SqlExecutionOptions safeOptions = requireOptions(options);
        requireSql(sql);
        if (safeOptions.maxRows() <= 0 && safeOptions.maxResultBytes() <= 0) {
            return protectTimeoutIfNeeded(source, safeOptions);
        }

        Flux<T> protectedSource = Flux.defer(() -> {
            AtomicLong resultBytes = new AtomicLong();
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
                                 long itemBytes = estimateBytes(rowSizer, tuple.getT2());
                                 long attemptedBytes = saturatingAdd(resultBytes.get(), itemBytes);
                                 // Long.MAX_VALUE 同时表示估算失败或累计溢出；即使上限也取最大值也必须失败关闭。
                                 if (attemptedBytes == Long.MAX_VALUE
                                         || attemptedBytes > safeOptions.maxResultBytes()) {
                                     sink.error(new SqlResultMemoryLimitExceededException(
                                             SqlStatementType.fromSql(sql),
                                             safeOptions.maxResultBytes(),
                                             attemptedBytes,
                                             rowIndex));
                                     return;
                                 }
                                 resultBytes.set(attemptedBytes);
                             }
                             sink.next(tuple.getT2());
                         });
        });
        return protectTimeoutIfNeeded(protectedSource, safeOptions);
    }

    private static <T> long estimateBytes(ToLongFunction<T> rowSizer, T row) {
        try {
            return Math.max(0L, rowSizer.applyAsLong(row));
        } catch (RuntimeException failure) {
            VirtualMachineError fatal = findVirtualMachineError(failure);
            if (fatal != null) {
                throw fatal;
            }
            // 无法检查结果大小时必须失败关闭，不能将未知大小按零字节放行而绕过内存保护。
            return Long.MAX_VALUE;
        }
    }

    private static <T> Flux<T> protectTimeoutIfNeeded(Flux<T> source, SqlExecutionOptions options) {
        if (options.timeout().isZero()) {
            return source;
        }
        Duration timeout = options.timeout();
        return SqlExecutionTimeouts.total(source, timeout);
    }

    private static SqlExecutionOptions requireOptions(SqlExecutionOptions options) {
        return Objects.requireNonNull(options, "sql execution options must not be null");
    }

    private static void requireSql(String sql) {
        Objects.requireNonNull(sql, "sql must not be null");
    }

    private static long saturatingAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }
}
