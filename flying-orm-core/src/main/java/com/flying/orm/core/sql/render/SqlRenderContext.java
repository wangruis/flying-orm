package com.flying.orm.core.sql.render;

import com.flying.orm.core.codec.ValueCodecRegistry;

/**
 * SQL 渲染上下文提供 term handler 在渲染局部条件时需要的公共能力。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public interface SqlRenderContext {

    /**
     * 渲染标识符。当前 MVP 仅做基础校验，后续由方言实现处理引用符和保留字。
     *
     * @param name 标识符名称
     * @return 渲染后的标识符
     */
    String identifier(String name);

    /**
     * 把 term 里的 Java 值转换成数据库参数。
     *
     * <p>默认实现保持原来的标准转换行为。由 {@link SqlRenderer} 创建的上下文会改用应用配置的
     * 注册表，因此内置条件和业务 term 能共享同一套枚举、Boolean、值对象转换规则。</p>
     *
     * @param value 条件里的原始值
     * @return 可以交给 SQL 请求继续绑定的值
     */
    default Object parameter(Object value) {
        return ValueCodecRegistry.standard().write(value);
    }
}
