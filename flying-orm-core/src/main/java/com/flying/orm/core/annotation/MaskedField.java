package com.flying.orm.core.annotation;

import com.flying.orm.core.protection.SensitiveDisplayMode;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 显式声明实体字段需要按通用 masking policy 控制业务结果展示。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface MaskedField {

    /** @return masking policy 稳定 ID */
    String policy() default "partial";

    /** @return 保留的前缀 code point 数 */
    int prefix() default 0;

    /** @return 保留的后缀 code point 数 */
    int suffix() default 4;

    /** @return 字段声明的默认展示方式 */
    SensitiveDisplayMode display() default SensitiveDisplayMode.MASKED;
}
