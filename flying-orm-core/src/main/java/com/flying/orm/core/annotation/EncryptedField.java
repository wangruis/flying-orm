package com.flying.orm.core.annotation;

import com.flying.orm.core.protection.EncryptedSearchMode;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 显式声明实体字段需要加密入库，并选择允许的保护搜索方式。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface EncryptedField {

    /** @return 显式启用的搜索方式 */
    EncryptedSearchMode[] search() default {EncryptedSearchMode.EXACT};

    /** @return 规范化器稳定 ID */
    String normalizer() default "identity";

    /** @return 允许的固定后缀 code point 长度 */
    int[] suffixLengths() default {};

    /** @return 规范化结果最大 code point 数 */
    int maxNormalizedLength() default 4096;

    /** @return contains 查询最小 code point 数 */
    int containsMinLength() default 3;
}
