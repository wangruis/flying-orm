package com.flying.orm.core.lambda;

import java.io.Serializable;
import java.util.function.Function;

/**
 * 可序列化的实体属性引用，是 flying-orm Lambda API 唯一接受的字段标识。
 *
 * <p>业务代码应传入直接的方法引用，例如 {@code UserEntity::getUserId} 或 record 的
 * {@code UserEntity::userId}。普通 Lambda 表达式可能包含计算、分支或跨对象访问，无法稳定对应
 * 数据库列，因此解析器会在 SQL 生成前拒绝它，而不会退化成字符串猜测。</p>
 *
 * @param <T> 属性所属实体类型
 * @param <R> 属性的 Java 值类型
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
@FunctionalInterface
public interface EntityProperty<T, R> extends Function<T, R>, Serializable {
}
