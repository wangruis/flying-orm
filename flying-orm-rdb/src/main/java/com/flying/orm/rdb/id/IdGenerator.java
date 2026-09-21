package com.flying.orm.rdb.id;

/**
 * 为 {@code @TableId(type = ASSIGN_ID)} 生成主键。
 *
 * <p>生成器由一套 ORM 客户端显式持有，JDBC 与 R2DBC Repository 共用它，不使用全局可变注册表。
 * 实现必须能够被多个请求线程并发调用，并且不能返回 {@code null}。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
@FunctionalInterface
public interface IdGenerator {

    /**
     * 生成一个尚未写入数据库的主键。
     *
     * @param entityType 实体类型
     * @param propertyName 主键 Java 属性名
     * @param targetType 主键属性类型
     * @return 可转换成目标属性类型的非空主键
     */
    Object generate(Class<?> entityType, String propertyName, Class<?> targetType);

    /** 未配置生成器时使用的拒绝实现，保证 ASSIGN_ID 不会退化成不安全的隐式算法。 */
    static IdGenerator none() {
        return (entityType, propertyName, targetType) -> {
            throw new IllegalStateException("ASSIGN_ID needs an explicitly configured IdGenerator: "
                                                    + entityType.getName() + "." + propertyName);
        };
    }
}
