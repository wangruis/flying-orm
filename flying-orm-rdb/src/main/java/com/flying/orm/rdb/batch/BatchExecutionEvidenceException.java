package com.flying.orm.rdb.batch;

import java.util.Objects;

/**
 * 批量失败时携带截至失败点已经形成的不可变证据；不保留请求或输入。
 *
 * @author wangr
 * @version v3.2
 */
public final class BatchExecutionEvidenceException extends RuntimeException {

    private final BatchExecutionEvidence evidence;

    public BatchExecutionEvidenceException(String message,
                                           Throwable cause,
                                           BatchExecutionEvidence evidence) {
        super(Objects.requireNonNull(message, "batch evidence error message must not be null"), cause);
        this.evidence = Objects.requireNonNull(evidence, "partial batch evidence must not be null");
    }

    public BatchExecutionEvidence evidence() {
        return evidence;
    }
}
