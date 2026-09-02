package com.flying.orm.core.codec;

/**
 * 把可选数据库驱动自己的包装对象拆成普通 Java 值。
 *
 * <p>主项目不直接依赖每一种驱动类型。上层按实际驱动注册 adapter 后，解包结果继续走统一 codec，
 * 因而数值溢出、时间和 Boolean 等校验不会被绕开。实现必须无状态并可并发调用。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public interface DriverValueAdapter {

    boolean supports(Object value);

    Object unwrap(Object value);
}
