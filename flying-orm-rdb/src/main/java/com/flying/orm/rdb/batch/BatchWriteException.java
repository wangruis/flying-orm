package com.flying.orm.rdb.batch;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.util.Objects;

/**
 * BatchWriteException 在批量任务无法继续时保留已经确认的分片结果。
 *
 * @author wangr
 * @date 2026-07-23
 * @version v1.0
 */
public final class BatchWriteException extends RuntimeException implements OrmErrorReportProvider {

    private static final long serialVersionUID = 1L;

    private final BatchWriteResult result;

    /**
     * 创建带原始异常和批量结果的异常。
     *
     * @param message 异常说明
     * @param cause   原始异常
     * @param result  终止前已经确认的结果
     */
    public BatchWriteException(String message, Throwable cause, BatchWriteResult result) {
        super(Objects.requireNonNull(message, "batch write error message must not be null"),
              Objects.requireNonNull(cause, "batch write error cause must not be null"));
        this.result = Objects.requireNonNull(result, "batch write error result must not be null");
    }

    /**
     * 返回终止前已经确认的批量结果。
     *
     * @return 批量结果
     */
    public BatchWriteResult result() {
        return result;
    }

    /** @return 带提交模式和汇总状态的统一批量错误报告 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("BATCH",
                                  "WRITE_" + result.status().name(),
                                  result.mode().name(),
                                  null,
                                  null,
                                  "batch write failed");
    }
}
