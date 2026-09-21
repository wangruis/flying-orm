package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记枚举中真正要保存到数据库的成员字段。
 *
 * <p>例如状态枚举可以用一个 {@code code} 字段作为数据库值，而不依赖枚举常量的声明顺序。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v2.0.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface EnumValue {
}
