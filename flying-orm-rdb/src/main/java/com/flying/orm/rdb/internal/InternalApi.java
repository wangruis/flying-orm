package com.flying.orm.rdb.internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记为了模块内部跨包协作而不得不保持 {@code public} 的类型或方法。
 *
 * <p>Java 的包级可见性不能跨子包使用，而这些实现也不值得在多个包里各复制一份。标记后，它们不会进入
 * flying-orm 的公开 API 基线，也不承诺版本兼容；业务代码不应该直接依赖。</p>
 *
 * @author wangr
 * @version v1.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface InternalApi {
}
