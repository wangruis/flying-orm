package com.flying.orm.rdb.batch;

/** 同一个 operationId 被拿来执行不同参数时抛出，防止错误复用旧回执。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class BatchReceiptMismatchException extends RuntimeException {

    public BatchReceiptMismatchException(String operationId) {
        super("batch receipt payload does not match the existing operation");
    }
}
