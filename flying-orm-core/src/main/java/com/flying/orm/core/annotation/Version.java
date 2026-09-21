package com.flying.orm.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记实体的乐观锁版本字段。
 *
 * <p>它只声明哪个字段是版本号，具体的版本比较和递增由 ORM 的写入层统一处理。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v2.0.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Version {
}
