package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * 一张表的命名主键定义。
 *
 * <p>复合主键的列顺序会影响数据库索引布局，因此这里保留调用方的声明顺序，构造后不再允许修改。</p>
 *
 * @param name 主键约束名
 * @param columns 主键列，顺序与声明一致
 * @author wangr
 * @version v3.2
 */
public record PrimaryKeyDefinition(String name, List<String> columns) {

    public PrimaryKeyDefinition {
        name = Names.requireText(name, "primary key name");
        columns = normalize(columns, "primary key column name");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("primary key columns must not be empty");
        }
        if (new HashSet<>(columns).size() != columns.size()) {
            throw new IllegalArgumentException("primary key columns must not contain duplicates");
        }
    }

    /** 创建命名主键，参数顺序就是复合主键的列顺序。 */
    public static PrimaryKeyDefinition of(String name, String... columns) {
        return new PrimaryKeyDefinition(name, Arrays.asList(columns));
    }

    private static List<String> normalize(List<String> columns, String fieldName) {
        if (columns == null) {
            throw new IllegalArgumentException("primary key columns must not be null");
        }
        return columns.stream().map(column -> Names.requireText(column, fieldName)).toList();
    }
}
