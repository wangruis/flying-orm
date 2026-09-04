package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.Objects;

/**
 * 索引中的一个有序列键。只表达列名和排序方向，不接受可直接拼入 SQL 的文本。
 *
 * @param column 列名
 * @param direction 排序方向
 * @author wangr
 * @version v3.2
 */
public record IndexKeyPart(String column, Direction direction) {

    public IndexKeyPart {
        column = Names.requireText(column, "index key column name");
        direction = Objects.requireNonNull(direction, "index key direction must not be null");
    }

    /** 创建升序索引键。 */
    public static IndexKeyPart asc(String column) {
        return new IndexKeyPart(column, Direction.ASC);
    }

    /** 创建降序索引键。 */
    public static IndexKeyPart desc(String column) {
        return new IndexKeyPart(column, Direction.DESC);
    }

    public enum Direction {
        ASC,
        DESC
    }
}
