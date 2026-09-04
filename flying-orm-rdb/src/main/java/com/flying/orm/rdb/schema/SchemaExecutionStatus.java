package com.flying.orm.rdb.schema;

/**
 * 审核结构计划及其单步执行的稳定结果状态。
 *
 * @author wangr
 * @version v3.2
 */
public enum SchemaExecutionStatus {
    SUCCESS,
    EXTERNAL_TRANSACTION_PENDING,
    PARTIAL,
    FAILED,
    UNKNOWN,
    PRECONDITION_FAILED,
    VERIFICATION_FAILED
}
