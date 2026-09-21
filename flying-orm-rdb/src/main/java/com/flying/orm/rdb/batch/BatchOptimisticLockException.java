package com.flying.orm.rdb.batch;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.util.List;
import java.util.Objects;

/**
 * 批量乐观锁至少有一行没有按预期更新时抛出。
 *
 * <p>异常不包含 SQL 参数，使用方可以通过 {@link #conflicts()} 拿到安全的输入偏移和影响行数。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class BatchOptimisticLockException extends RuntimeException implements OrmErrorReportProvider {

    private static final long serialVersionUID = 1L;

    private final List<BatchRowConflict> conflicts;

    public BatchOptimisticLockException(List<BatchRowConflict> conflicts) {
        super(message(conflicts));
        this.conflicts = List.copyOf(Objects.requireNonNull(conflicts, "batch conflicts must not be null"));
        if (this.conflicts.isEmpty()) {
            throw new IllegalArgumentException("batch optimistic lock conflicts must not be empty");
        }
    }

    public List<BatchRowConflict> conflicts() {
        return conflicts;
    }

    /** @return 定位到首个冲突输入行的统一报告，完整冲突列表仍通过 conflicts() 获取 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("OPTIMISTIC_LOCK",
                                  "BATCH_OPTIMISTIC_LOCK",
                                  null,
                                  "rows[" + conflicts.getFirst().inputOffset() + "]",
                                  null,
                                  getMessage());
    }

    private static String message(List<BatchRowConflict> conflicts) {
        List<BatchRowConflict> safeConflicts = Objects.requireNonNull(conflicts, "batch conflicts must not be null");
        return "batch optimistic lock conflict: count=" + safeConflicts.size();
    }
}
