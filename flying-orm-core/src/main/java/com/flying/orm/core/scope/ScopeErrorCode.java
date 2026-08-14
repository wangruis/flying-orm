package com.flying.orm.core.scope;

/**
 * 数据范围校验的稳定错误码。上层按错误码处理业务，不需要解析异常文本。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public enum ScopeErrorCode {

    /** 调用缺少表单要求的可信租户范围。 */
    TENANT_SCOPE_REQUIRED,

    /** 租户策略已经开启，但表单没有对应租户字段。 */
    TENANT_FIELD_REQUIRED,

    /** 写入数据携带的租户值与可信租户范围冲突。 */
    TENANT_VALUE_MISMATCH,

    /** 同一行或同一批布局重复声明了租户字段。 */
    DUPLICATE_TENANT_FIELD,

    /** 字段范围校验缺少动态表单字段元数据。 */
    FORM_FIELDS_REQUIRED,

    /** 字段裁剪后没有任何字段可以读取。 */
    NO_READABLE_FIELDS,

    /** 查询明确请求了不可读字段。 */
    FIELD_NOT_READABLE,

    /** 写入明确请求了不可写字段。 */
    FIELD_NOT_WRITABLE
}
