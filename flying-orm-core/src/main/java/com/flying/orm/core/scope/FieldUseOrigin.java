package com.flying.orm.core.scope;

/**
 * 字段用途来自调用方，还是来自 ORM 为保证正确性和隔离性注入的内部结构。
 *
 * <p>origin 是授权键的一部分。允许 INTERNAL_TENANT 使用 tenant_id，不会顺带允许 CALLER
 * 投影或过滤 tenant_id，也不会授权另一种 INTERNAL_* origin。</p>
 *
 * @author wangr
 * @version v3.2
 */
public enum FieldUseOrigin {
    CALLER,
    INTERNAL_SCOPE,
    INTERNAL_TENANT,
    INTERNAL_LOGIC_DELETE,
    INTERNAL_VERSION,
    INTERNAL_TIE_BREAKER;

    /** @return true 表示用途由 ORM 内部注入，不会成为调用方字段权限 */
    public boolean internal() {
        return this != CALLER;
    }
}
