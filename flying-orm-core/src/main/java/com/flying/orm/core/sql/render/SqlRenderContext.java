package com.flying.orm.core.sql.render;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;

/**
 * SQL 渲染上下文提供 term handler 在渲染局部条件时需要的公共能力。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public interface SqlRenderContext {

    /**
     * 校验并渲染标识符；方言化上下文负责引用符和限定名。
     *
     * @param name 标识符名称
     * @return 渲染后的标识符
     */
    String identifier(String name);

    /**
     * 返回当前条件字段的已解析声明；关联字段取所属来源，HAVING 字段取聚合输出声明。
     * 仅表名/原生 SQL 上下文没有字段模型，不能猜测字段类型。
     *
     * @param name 当前条件的字段名称
     * @return 字段声明
     */
    default DynamicField field(String name) {
        throw new IllegalStateException("SQL render context has no field metadata");
    }

    /**
     * 把 term 里的 Java 值转换成数据库参数。
     *
     * <p>默认实现使用标准转换。由 {@link SqlRenderer} 创建的上下文会改用应用配置的
     * 注册表，因此内置条件和业务 term 能共享同一套枚举、Boolean、值对象转换规则。</p>
     *
     * @param value 条件里的原始值
     * @return 可以交给 SQL 请求继续绑定的值
     */
    default Object parameter(Object value) {
        return ValueCodecRegistry.standard().write(value);
    }
}
