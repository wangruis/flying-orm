package com.flying.orm.core.form;

/**
 * 表单的租户处理方式。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public enum TenantStrategy {

    /**
     * 这张表不参与租户隔离，适合单租户系统或公共表。
     */
    NONE,

    /**
     * 租户值由上层已确认的 TenantScope 自动补进写入数据。
     */
    AUTO,

    /**
     * 调用方必须明确给出租户值和租户范围，ORM 只负责核对是否一致。
     */
    MANUAL
}
