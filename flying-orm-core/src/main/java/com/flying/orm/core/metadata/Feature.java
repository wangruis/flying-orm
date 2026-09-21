package com.flying.orm.core.metadata;

/**
 * Feature 表示可挂载到元数据或运行时对象上的扩展能力，要求 id 在同一注册表内唯一。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public interface Feature {

    /**
     * 返回 feature 的稳定标识。
     *
     * @return feature 标识
     */
    String id();
}
