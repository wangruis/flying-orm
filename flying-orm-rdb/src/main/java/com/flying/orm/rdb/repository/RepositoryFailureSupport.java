package com.flying.orm.rdb.repository;

import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import com.flying.orm.rdb.execution.GeneratedKeyReadException;

import java.util.Objects;

/**
 * Repository 写入事实与 JVM 致命错误的异常转换。
 *
 * <p>生成键读取失败保留数据库已报告的影响行数；受支持的异步包装保留其 JVM 致命错误。
 * 不执行重试、恢复或补偿，无共享可变状态。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class RepositoryFailureSupport {

    private RepositoryFailureSupport() {
    }

    /** 生成键读取失败保留实际影响行数，并从已知读取包装中恢复 JVM 致命错误。 */
    static RuntimeException generatedKeyReadFailure(GeneratedKeyReadException failure) {
        Throwable preferred = preferVirtualMachineError(failure.getCause());
        if (preferred instanceof VirtualMachineError fatal) {
            throw fatal;
        }
        return new GeneratedKeyResolutionException(
                failure.affectedRows(), GeneratedKeyResolutionException.Phase.READ, failure);
    }

    /** 仅供真实 Reactor/JDK 异步错误边界恢复受支持包装器中的 VME。 */
    static Throwable preferVirtualMachineError(Throwable failure) {
        Throwable safeFailure = Objects.requireNonNull(failure, "repository failure must not be null");
        VirtualMachineError fatal = findVirtualMachineError(safeFailure);
        return fatal == null ? safeFailure : fatal;
    }

}
