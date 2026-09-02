package com.flying.orm.rdb.internal.mapping;

import java.util.Objects;

/**
 * 实体名和字段名没有注解时，用这里的约定转成表名、列名。
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
interface EntityNamingStrategy {

    EntityNamingStrategy SNAKE_CASE = new SnakeCaseEntityNamingStrategy();

    /**
     * 根据实体类名推表名。
     *
     * @param type 实体类型
     * @return 表名
     */
    String tableName(Class<?> type);

    /**
     * 根据 Java 字段名推列名。
     *
     * @param propertyName Java 字段名
     * @return 列名
     */
    String columnName(String propertyName);

}

/** 默认命名算法的实现不需要公开；调用方通过 EntityNamingStrategy.SNAKE_CASE 使用即可。 */
final class SnakeCaseEntityNamingStrategy implements EntityNamingStrategy {

    @Override
    public String tableName(Class<?> type) {
        Class<?> safeType = Objects.requireNonNull(type, "entity type must not be null");
        return columnName(safeType.getSimpleName());
    }

    @Override
    public String columnName(String propertyName) {
        String text = Objects.requireNonNull(propertyName, "property name must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException("property name must not be blank");
        }
        StringBuilder name = new StringBuilder(text.length() + 8);
        for (int i = 0; i < text.length(); i++) {
            char current = text.charAt(i);
            if (Character.isUpperCase(current)) {
                if (i > 0 && name.charAt(name.length() - 1) != '_') {
                    name.append('_');
                }
                name.append(Character.toLowerCase(current));
            } else if (current == '-' || current == ' ') {
                if (!name.isEmpty() && name.charAt(name.length() - 1) != '_') {
                    name.append('_');
                }
            } else {
                name.append(current);
            }
        }
        return name.toString();
    }
}
