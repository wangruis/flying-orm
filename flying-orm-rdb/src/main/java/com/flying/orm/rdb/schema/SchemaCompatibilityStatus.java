package com.flying.orm.rdb.schema;

/**
 * 一次结构兼容判断的结果。
 *
 * <p>{@link #INCOMPATIBLE} 是明确结果，不是允许调用方强制继续的模式。报告保留完整 operation，
 * 上层可以展示和审核原因，但不能把失败结果解释成执行授权。</p>
 *
 * @author wangr
 * @version v3.2
 */
public enum SchemaCompatibilityStatus {
    COMPATIBLE,
    INCOMPATIBLE
}
