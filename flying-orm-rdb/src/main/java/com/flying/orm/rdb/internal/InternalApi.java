package com.flying.orm.rdb.internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记为了模块内部跨包协作而不得不保持 {@code public} 的类型或方法。
 *
 * <p>Java 的包级可见性不能跨子包使用，而这些实现也不值得在多个包里各复制一份。
 * 内部实现不属于承诺版本兼容的公开 API，业务代码不应该直接依赖。</p>
 *
 * <p>internal 包内的类型天然属于内部实现，无需重复标记。包外标记类型时，
 * 该类型声明的成员及嵌套类型都属于内部实现，无需逐个重复标记。
 * 正式公开类型中的内部方法仍须单独标记。API 检查工具应同时识别 internal 包、成员自身和所属类型
 * （包括外层类型）；Java 反射不会把类型注解自动复制到成员上。</p>
 *
 * @author wangr
 * @version v1.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface InternalApi {
}
