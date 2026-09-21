package com.flying.orm.rdb.lifecycle;

/**
 * 实体一次持久化操作所处的阶段。
 *
 * <p>阶段名和常见持久化注解保持一致，但这是 flying-orm 自己的 API，上层不带 JPA 也能直接使用。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum EntityLifecyclePhase {
    PRE_PERSIST,
    POST_PERSIST,
    PRE_UPDATE,
    POST_UPDATE,
    PRE_REMOVE,
    POST_REMOVE,
    POST_LOAD
}
