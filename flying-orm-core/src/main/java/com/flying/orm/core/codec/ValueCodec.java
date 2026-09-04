package com.flying.orm.core.codec;

import java.util.Optional;

/**
 * 单个类型转换器。写库前把 Java 值变成数据库友好的值，读库后再转回调用方要的类型。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public interface ValueCodec {

    /**
     * 可配置装配所需的稳定适用范围。空值表示既有 trusted startup codec，不改变历史行为。
     */
    default Optional<ValueCodecDescriptor> descriptor() {
        return Optional.empty();
    }

    /**
     * 判断这个转换器能不能处理目标类型。
     *
     * @param targetType 调用方想要的 Java 类型
     * @return 能处理就返回 true
     */
    boolean supports(Class<?> targetType);

    /**
     * 写库前转换。默认直接透传，像枚举这种需要稳定字符串的类型再覆盖它。
     *
     * @param value Java 侧原始值
     * @return 数据库绑定参数值
     */
    default Object write(Object value) {
        return value;
    }

    /**
     * 读库后转换。
     *
     * @param value      数据库返回值
     * @param targetType 调用方想要的 Java 类型
     * @return 转换后的 Java 值
     */
    Object read(Object value, Class<?> targetType);
}
